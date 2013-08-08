package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericUDTFColumnNameTest extends GenericUDTF {

  private List<String> colNames;

  @Override
  public StructObjectInspector initialize(StructObjectInspector inputOI)
      throws UDFArgumentException {
    List<String> colNames = new ArrayList<String>();
    List<ObjectInspector> colOIs = new ArrayList<ObjectInspector>();
    for (StructField field : inputOI.getAllStructFieldRefs()) {
      colNames.add(field.getFieldName());
      colOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    this.colNames = colNames;
    return ObjectInspectorFactory.getStandardStructObjectInspector(colNames, colOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    forward(colNames);
  }

  @Override
  public void close() throws HiveException {
  }

  public String toString() {
    return "dummy_udtf_for_printing_col_names";
  }
}
