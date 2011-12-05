package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

@Description(name = "between", value = "_FUNC_ a [NOT] BETWEEN b AND c - evaluate if a is [not] in between a and b")
public class GenericUDFBetween extends GenericUDF {

  GenericUDFOPEqualOrGreaterThan egt = new GenericUDFOPEqualOrGreaterThan();
  GenericUDFOPEqualOrLessThan elt = new GenericUDFOPEqualOrLessThan();

  private ObjectInspector[] argumentOIs;
  private final BooleanWritable result = new BooleanWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (!arguments[0].getTypeName().equals("boolean")) {
      throw new UDFArgumentTypeException(0, "First argument for BETWEEN should be boolean type");
    }
    egt.initialize(new ObjectInspector[] {arguments[1], arguments[2]});
    elt.initialize(new ObjectInspector[] {arguments[1], arguments[3]});

    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    boolean invert = (Boolean) ((PrimitiveObjectInspector) argumentOIs[0])
				.getPrimitiveJavaObject(arguments[0].get());

    BooleanWritable left = ((BooleanWritable)egt.evaluate(new DeferredObject[] {arguments[1], arguments[2]}));
    if (left == null) {
      return null;
    }
    if (!invert && !left.get()) {
      result.set(false);
      return result;
    }
    BooleanWritable right = ((BooleanWritable)elt.evaluate(new DeferredObject[] {arguments[1], arguments[3]}));
    if (right == null) {
      return null;
    }
    boolean between = left.get() && right.get();
    result.set(invert ? !between : between);
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append(children[1]);
    if (Boolean.valueOf(children[0])) {
      sb.append(" NOT");
    }
    sb.append(" BETWEEN ");
    sb.append(children[2]).append(" AND ").append(children[3]);
    return sb.toString();
  }
}
