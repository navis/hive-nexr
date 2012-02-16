package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class GenericUDFTestClose extends GenericUDF {

  private String prefix;
  private ObjectInspector[] argumentOIs;

  private Text wrapper = new Text();

  @Override
  public ObjectInspector initialize(Configuration conf, ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (conf != null) {
      prefix = conf.get("user.defined", "not-defined");
    }
    return initialize(arguments);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    this.argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    wrapper.set(prefix + " : " +
        ((StringObjectInspector) argumentOIs[0]).getPrimitiveJavaObject(arguments[0].get()));
    return wrapper;
  }

  @Override
  public void close() {
    if (prefix != null && prefix.equals("negative")) {
      System.out.println(0 / 0);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "GenericUDFTestClose(" + children[0] + ")";
  }
}
