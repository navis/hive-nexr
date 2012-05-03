package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class Registry {

  /**
   * The mapping from expression function names to expression classes.
   */
  final Map<String, FunctionInfo> mFunctions = new LinkedHashMap<String, FunctionInfo>();
  final boolean isNative;

  Registry(boolean isNative) {
    this.isNative = isNative;
  }

  public Registry() {
    this(false);
  }

  /**
   * Registers Hive functions from a plugin jar, using metadata from
   * the jar's META-INF/class-info.xml.
   *
   * @param jarLocation URL for reading jar file
   * @param classLoader classloader to use for loading function classes
   */
  public void registerFunctionsFromPluginJar(
      URL jarLocation,
      ClassLoader classLoader) throws Exception {

    URL url = new URL("jar:" + jarLocation + "!/META-INF/class-info.xml");
    InputStream inputStream = null;
    try {
      inputStream = url.openStream();
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = dbf.newDocumentBuilder();
      Document doc = docBuilder.parse(inputStream);
      Element root = doc.getDocumentElement();
      if (!root.getTagName().equals("ClassList")) {
        return;
      }
      NodeList children = root.getElementsByTagName("Class");
      for (int i = 0; i < children.getLength(); ++i) {
        Element child = (Element) children.item(i);
        String javaName = child.getAttribute("javaname");
        String sqlName = child.getAttribute("sqlname");
        Class<?> udfClass = Class.forName(javaName, true, classLoader);
        boolean registered = registerFunction(sqlName, udfClass);
        if (!registered) {
          throw new RuntimeException(
              "Class " + udfClass + " is not a Hive function implementation");
        }
      }
    } finally {
      IOUtils.closeStream(inputStream);
    }
  }


  /**
   * Registers the appropriate kind of temporary function based on a class's
   * type.
   *
   * @param functionName name under which to register function
   * @param udfClass     class implementing UD[A|T]F
   * @return true if udfClass's type was recognized (so registration
   *         succeeded); false otherwise
   */
  @SuppressWarnings("unchecked")
  public boolean registerFunction(
      String functionName, Class<?> udfClass) {

    if (UDF.class.isAssignableFrom(udfClass)) {
      registerUDF(
          functionName, (Class<? extends UDF>) udfClass, false);
    } else if (GenericUDF.class.isAssignableFrom(udfClass)) {
      registerGenericUDF(
          functionName, (Class<? extends GenericUDF>) udfClass);
    } else if (GenericUDTF.class.isAssignableFrom(udfClass)) {
      registerGenericUDTF(
          functionName, (Class<? extends GenericUDTF>) udfClass);
    } else if (UDAF.class.isAssignableFrom(udfClass)) {
      registerUDAF(
          functionName, (Class<? extends UDAF>) udfClass);
    } else if (GenericUDAFResolver.class.isAssignableFrom(udfClass)) {
      registerGenericUDAF(
          functionName, (GenericUDAFResolver)
          ReflectionUtils.newInstance(udfClass, null));
    } else {
      return false;
    }
    return true;
  }

  public void registerUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator) {
    registerUDF(functionName, UDFClass, isOperator, functionName
        .toLowerCase());
  }

  public void registerUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
    if (UDF.class.isAssignableFrom(UDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, displayName,
          new GenericUDFBridge(displayName, isOperator, UDFClass));
      addfunction(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass
          + " which does not extend " + UDF.class);
    }
  }

  public void registerGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    if (GenericUDF.class.isAssignableFrom(genericUDFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          ReflectionUtils.newInstance(genericUDFClass, null));
      addfunction(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDF Class "
          + genericUDFClass + " which does not extend " + GenericUDF.class);
    }
  }

  public void registerGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    if (GenericUDTF.class.isAssignableFrom(genericUDTFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          ReflectionUtils.newInstance(genericUDTFClass, null));
      addfunction(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDTF Class "
          + genericUDTFClass + " which does not extend " + GenericUDTF.class);
    }
  }

  public synchronized FunctionInfo getFunctionInfo(String functionName) {
    return mFunctions.get(functionName.toLowerCase());
  }

  /**
   * Returns a set of registered function names. This is used for the CLI
   * command "SHOW FUNCTIONS;"
   *
   * @return set of strings contains function names
   */
  public synchronized Set<String> getFunctionNames() {
    return mFunctions.keySet();
  }

  /**
   * Returns a set of registered function names. This is used for the CLI
   * command "SHOW FUNCTIONS 'regular expression';" Returns an empty set when
   * the regular expression is not valid.
   *
   * @param funcPatternStr regular expression of the interested function names
   * @return set of strings contains function names
   */
  public Set<String> getFunctionNames(String funcPatternStr) {
    Set<String> funcNames = new TreeSet<String>();
    Pattern funcPattern;
    try {
      funcPattern = Pattern.compile(funcPatternStr);
    } catch (PatternSyntaxException e) {
      return funcNames;
    }
    for (String funcName : mFunctions.keySet()) {
      if (funcPattern.matcher(funcName).matches()) {
        funcNames.add(funcName);
      }
    }
    return funcNames;
  }

  /**
   * Returns the set of synonyms of the supplied function.
   *
   * @param funcName the name of the function
   * @return Set of synonyms for funcName
   */
  public synchronized Set<String> getFunctionSynonyms(String funcName) {
    Set<String> synonyms = new HashSet<String>();

    FunctionInfo funcInfo = getFunctionInfo(funcName);
    if (null == funcInfo) {
      return synonyms;
    }

    Class<?> funcClass = funcInfo.getFunctionClass();
    for (String name : mFunctions.keySet()) {
      if (name.equals(funcName)) {
        continue;
      }
      if (mFunctions.get(name).getFunctionClass().equals(funcClass)) {
        synonyms.add(name);
      }
    }

    return synonyms;
  }

  /**
   * Get the GenericUDAF evaluator for the name and argumentClasses.
   *
   * @param name         the name of the UDAF
   * @param argumentOIs
   * @param isDistinct
   * @param isAllColumns
   * @return The UDAF evaluator
   */
  @SuppressWarnings("deprecation")
  public GenericUDAFEvaluator getGenericUDAFEvaluator(String name,
      List<ObjectInspector> argumentOIs, boolean isDistinct,
      boolean isAllColumns) throws SemanticException {

    GenericUDAFResolver udafResolver = getGenericUDAFResolver(name);
    if (udafResolver == null) {
      return null;
    }

    GenericUDAFEvaluator udafEvaluator;
    ObjectInspector args[] = new ObjectInspector[argumentOIs.size()];
    // Can't use toArray here because Java is dumb when it comes to
    // generics + arrays.
    for (int ii = 0; ii < argumentOIs.size(); ++ii) {
      args[ii] = argumentOIs.get(ii);
    }

    GenericUDAFParameterInfo paramInfo =
        new SimpleGenericUDAFParameterInfo(
            args, isDistinct, isAllColumns);
    if (udafResolver instanceof GenericUDAFResolver2) {
      udafEvaluator =
          ((GenericUDAFResolver2) udafResolver).getEvaluator(paramInfo);
    } else {
      udafEvaluator = udafResolver.getEvaluator(paramInfo.getParameters());
    }
    return udafEvaluator;
  }

  public void registerGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    addfunction(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), genericUDAFResolver));
  }

  public void registerUDAF(String functionName, Class<? extends UDAF> udafClass) {
    addfunction(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), new GenericUDAFBridge(
        ReflectionUtils.newInstance(udafClass, null))));
  }

  private synchronized void addfunction(String name, FunctionInfo function) {
    if (isNative && mFunctions.containsKey(name)) {
      throw new RuntimeException("Function " + name + " is hive native, it can't be replaced");
    }
    mFunctions.put(name, function);
  }

  public synchronized void unregisterUDF(String functionName) throws HiveException {
    FunctionInfo fi = mFunctions.get(functionName.toLowerCase());
    if (fi != null) {
      if (!fi.isNative()) {
        mFunctions.remove(functionName.toLowerCase());
      } else {
        throw new HiveException("Function " + functionName
            + " is hive native, it can't be dropped");
      }
    }
  }

  public GenericUDAFResolver getGenericUDAFResolver(String functionName) {
    FunctionInfo finfo = getFunctionInfo(functionName);
    if (finfo != null) {
      return finfo.getGenericUDAFResolver();
    }
    return null;
  }

  public synchronized void clear() {
    if (isNative) {
      throw new IllegalStateException("Native function registry could not be cleared");
    }
    mFunctions.clear();
  }
}
