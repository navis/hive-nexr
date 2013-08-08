package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericUDFColumnNameTest extends GenericUDF {

  private String colNames;

  @Override
  public ObjectInspector initialize(StructObjectInspector inputOI)
    throws UDFArgumentException {
    StringBuilder builder = new StringBuilder();
    for (StructField field : inputOI.getAllStructFieldRefs()) {
      if (builder.length() > 0) {
        builder.append(", ");
      }
      builder.append(field.getFieldName());
    }
    colNames = builder.toString();
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return colNames;
  }

  @Override
  public String getDisplayString(String[] children) {
    return toDisplayString("dummy_udf_for_printing_col_names", children);
  }
}
