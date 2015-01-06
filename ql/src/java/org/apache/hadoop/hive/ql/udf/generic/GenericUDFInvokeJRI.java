package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.JRIEngine;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.StringUtils;
import org.rosuda.JRI.RBool;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * select udf_jri(
 * 'rfunc <- function(p1, p2) { p1 * p2 }', 
 * "rfunc", array(1,2,3), array(2,3,4)) from src tablesample (1 rows);
 * 
 * or
 * 
 * set R1='rfunc <- function(p1, p2) { p1 * p2 }';
 * 
 * select udf_jri(${hiveconf:R1}, 
 * 'rfunc', array(1,2,3), array(2,3,4)) from src tablesample (1 rows);
 */
public class GenericUDFInvokeJRI extends GenericUDF {

  private static enum InputType {
    STRING,
    STRING_ARRAY { ObjectInspector getElementOI() { return PrimitiveObjectInspectorFactory.javaStringObjectInspector; } },
    BOOLEAN_ARRAY { ObjectInspector getElementOI() { return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector; } },
    INT_ARRAY { ObjectInspector getElementOI() { return PrimitiveObjectInspectorFactory.javaIntObjectInspector; } },
    DOUBLE_ARRAY { ObjectInspector getElementOI() { return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector; } };

    ObjectInspector getElementOI() { throw new UnsupportedOperationException(); }
  }

  private static enum OutputType {
    STRING { ObjectInspector getOI() { return PrimitiveObjectInspectorFactory.javaStringObjectInspector; } },
    BOOLEAN { ObjectInspector getOI() { return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector; } },
    INT { ObjectInspector getOI() { return PrimitiveObjectInspectorFactory.javaIntObjectInspector; } },
    DOUBLE { ObjectInspector getOI() { return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector; } },
    STRING_ARRAY { ObjectInspector getOI() { return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector); } },
    INT_ARRAY { ObjectInspector getOI() { return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector); } },
    DOUBLE_ARRAY { ObjectInspector getOI() { return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector); } },
    DOUBLE_MATRIX { ObjectInspector getOI() { return ObjectInspectorFactory.getStandardListObjectInspector(
        ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)); } };
    
    ObjectInspector getOI() { throw new UnsupportedOperationException(); }
  }

  private String[] argNames;
  private ObjectInspector[] argOIs;
  private String evalString;

  private InputType[] inputTypes;
  private ObjectInspectorConverters.Converter[] inputConverters;

  private OutputType outputType;

  private int[] intArray;
  private double[] doubleArray;
  private String[] stringArray;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException("function is not defined");
    }
    Rengine engine = JRIEngine.init();
    if (!(arguments[0] instanceof WritableConstantStringObjectInspector)) {
      throw new UDFArgumentTypeException(0, "function should be a constant string");
    }
    if (!(arguments[1] instanceof WritableConstantStringObjectInspector)) {
      throw new UDFArgumentTypeException(1, "function name should be a constant string");
    }
    String script = ObjectInspectorUtils.getConstantString(arguments[0]);
    String functionName = ObjectInspectorUtils.getConstantString(arguments[1]);

    engine.getRsync().lock();
    try {
      evaluateScript(engine, script);

      argNames = parseArgumentNames(script, functionName);
      evalString = functionName + "(" + StringUtils.join(", ", argNames) + ")";

      initOI(Arrays.copyOfRange(arguments, 2, arguments.length));
      
      REXP output = engine.eval(functionName + ".output()");
      if (output == null) {
        outputType = OutputType.valueOf(inputTypes[0].name());
      } else {
        String typeString = output.asString();
        int index = Arrays.asList(argNames).indexOf(typeString);
        if (index >= 0) {
          outputType = OutputType.valueOf(inputTypes[index].name());
        } else {
          outputType = getOutputType(TypeInfoUtils.getTypeInfoFromTypeString(typeString));
        }
      }
      return outputType.getOI();
    } finally {
      engine.getRsync().unlock();
    }
  }
  
  private String[] parseArgumentNames(String script, String functionName) throws UDFArgumentException {
    String regex = functionName + "\\s*<\\-\\s*function\\s*\\(([^\\)]*)\\).*";
    Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
    Matcher matcher = pattern.matcher(script);
    if (!matcher.matches()) {
      throw new UDFArgumentException("Failed to parse argument names");
    }
    String[] args = matcher.group(1).split(",");
    String[] output = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      output[i] = args[i].trim();
    }
    return output;
  }

  private void evaluateScript(Rengine engine, String script) throws UDFArgumentException {
    BufferedReader reader = new BufferedReader(new StringReader(script));
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        if (engine.eval(line) == null) {
          throw new UDFArgumentException("Failed to init script " + line); 
        }
      }
    } catch (IOException e) {
      throw new UDFArgumentException(e); 
    }
  }

  private void initOI(ObjectInspector[] arguments) throws UDFArgumentException {
    argOIs = arguments;
    inputTypes = new InputType[arguments.length];
    inputConverters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      InputType type;
      ObjectInspectorConverters.Converter converter;
      if (arguments[i] instanceof ListObjectInspector) {
        ListObjectInspector loi = (ListObjectInspector) arguments[i];
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (eoi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
          throw new UDFArgumentException("Invalid JRI type");
        }
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) eoi;
        inputTypes[i] = getArrayType(poi.getPrimitiveCategory());
        inputConverters[i] = ObjectInspectorConverters.getConverter(poi, inputTypes[i].getElementOI());
      } else {
        inputTypes[i] = InputType.STRING;
        inputConverters[i] = ObjectInspectorConverters.getConverter(
            arguments[i], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      }
    }
  }

  private OutputType getOutputType(TypeInfo typeInfo) throws UDFArgumentException {
    if (typeInfo.getCategory() == ObjectInspector.Category.LIST) {
      ListTypeInfo ltype = (ListTypeInfo) typeInfo;
      TypeInfo etype = ltype.getListElementTypeInfo();
      if (etype.getCategory() == ObjectInspector.Category.LIST) {
        ListTypeInfo lltype = (ListTypeInfo) etype;
        TypeInfo eetype = lltype.getListElementTypeInfo();
        if (eetype.getTypeName().equals(serdeConstants.DOUBLE_TYPE_NAME)) {
          return OutputType.DOUBLE_MATRIX;
        }
        throw new UDFArgumentException("Invalid JRI type " + typeInfo);
      }
      if (etype.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
        return OutputType.STRING_ARRAY;
      }
      if (etype.getTypeName().equals(serdeConstants.INT_TYPE_NAME)) {
        return OutputType.INT_ARRAY;
      } 
      if (etype.getTypeName().equals(serdeConstants.DOUBLE_TYPE_NAME)) {
        return OutputType.DOUBLE_ARRAY;
      } 
      throw new UDFArgumentException("Invalid JRI type " + typeInfo);
    }
    if (typeInfo.getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
      return OutputType.STRING;
    }
    if (typeInfo.getTypeName().equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
      return OutputType.BOOLEAN;
    }
    if (typeInfo.getTypeName().equals(serdeConstants.INT_TYPE_NAME)) {
      return OutputType.INT;
    }
    if (typeInfo.getTypeName().equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      return OutputType.DOUBLE;
    }
    throw new UDFArgumentException("Invalid JRI type " + typeInfo);
  }

  private InputType getArrayType(PrimitiveObjectInspector.PrimitiveCategory category)
      throws UDFArgumentTypeException {
    switch (category) {
      case STRING:
      case VARCHAR:
      case TIMESTAMP:
      case DATE:
        return InputType.STRING_ARRAY;
      case BOOLEAN:
        return InputType.BOOLEAN_ARRAY;
      case BYTE:
      case SHORT:
      case INT:
        return InputType.INT_ARRAY;
      case LONG:
      case FLOAT:
      case DOUBLE:
        return InputType.DOUBLE_ARRAY;
      default:
        throw new UDFArgumentTypeException(1, "Unsupported type " + category);
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Rengine engine = JRIEngine.init();
    engine.getRsync().lock();
    try {
      return evaluateR(engine, arguments);
    } finally {
      engine.getRsync().unlock();
    }
  }

  private Object evaluateR(Rengine engine, DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < inputTypes.length; i++) {
      Object input = arguments[i + 2].get();
      if (inputTypes[i] == InputType.STRING) {
        engine.assign(argNames[i], (String)inputConverters[i].convert(input));
        continue;
      }
      int length = ((ListObjectInspector)argOIs[i]).getListLength(input);
      List<?> list = ((ListObjectInspector)argOIs[i]).getList(input);
      switch (inputTypes[i]) {
        case BOOLEAN_ARRAY:
          boolean[] booleans = new boolean[length];
          for (int j = 0 ; j < length; j++) {
            booleans[j] = (Boolean)inputConverters[i].convert(list.get(j));
          }
          engine.assign(argNames[i], booleans);
          break;
        case INT_ARRAY:
          int[] ints = new int[length];
          for (int j = 0 ; j < length; j++) {
            ints[j] = (Integer)inputConverters[i].convert(list.get(j));
          }
          engine.assign(argNames[i], ints);
          break;
        case DOUBLE_ARRAY:
          double[] doubles = new double[length];
          for (int j = 0 ; j < length; j++) {
            doubles[j] = (Double)inputConverters[i].convert(list.get(j));
          }
          engine.assign(argNames[i], doubles);
          break;
        case STRING_ARRAY:
          String[] strings = new String[length];
          for (int j = 0 ; j < length; j++) {
            strings[j] = (String)inputConverters[i].convert(list.get(j));
          }
          engine.assign(argNames[i], strings);
          break;
      }
    }
    REXP x = engine.eval(evalString);
    if (x == null) {
      return null;
    }
    switch (outputType) {
      case STRING:
        return x.asString();
      case BOOLEAN:
        return convert(x.asBool());
      case INT:
        return x.asInt();
      case DOUBLE:
        return x.asDouble();
      case STRING_ARRAY:
        return x.asStringArray();
      case INT_ARRAY:
        return convert(x.asIntArray());
      case DOUBLE_ARRAY:
        return convert(x.asDoubleArray());
      case DOUBLE_MATRIX:
        return convert(x.asDoubleMatrix());
    }
    return null;
  }

  private Object convert(RBool rBool) {
    return rBool == null || rBool.isNA() ? null : rBool.isTRUE(); 
  }

  private Integer[] convert(int[] ints) {
    if (ints == null) {
      return null;
    }
    Integer[] array = new Integer[ints.length];
    for (int i = 0; i < ints.length; i++) {
      array[i] = ints[i];
    }
    return array;
  }

  private Double[][] convert(double[][] doubles) {
    if (doubles == null) {
      return null;
    }
    Double[][] array = new Double[doubles.length][];
    for (int i = 0; i < doubles.length; i++) {
      array[i] = convert(doubles[i]);
    }
    return array;
  }

  private Double[] convert(double[] doubles) {
    if (doubles == null) {
      return null;
    }
    Double[] array = new Double[doubles.length];
    for (int i = 0; i < doubles.length; i++) {
      array[i] = doubles[i];
    }
    return array;
  }

  public static void main(String[] args) throws Exception {
    String a = 
        "rfunc <- function(p1, p2) { p1 + p2 }\n" +
            "rfunc.output <- function() { \"array<int>\" }";
    Matcher m = Pattern.compile(".*rfunc\\s*<\\-\\s*function\\s*\\(.*\\).*", Pattern.DOTALL).matcher(a);
    System.err.println("[GenericUDFInvokeJRI/main] " +m.matches());
  }
}
