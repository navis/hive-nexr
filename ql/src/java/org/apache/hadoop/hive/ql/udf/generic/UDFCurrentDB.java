package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

// deterministic in the query range
@Description(name = "current_database",
    value = "_FUNC_() - returns currently using database name")
public class UDFCurrentDB extends GenericUDF {

  private MapredContext context;

  @Override
  public void configure(MapredContext context) {
    this.context = context;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    try {
      String database;
      if (context != null) {
        database = context.getJobConf().get("hive.current.database");
      } else {
        database = org.apache.hadoop.hive.ql.metadata.Hive.get().getCurrentDatabase();
      }
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
          PrimitiveObjectInspector.PrimitiveCategory.STRING, new Text(database));
    } catch (HiveException e) {
      throw new UDFArgumentException(e);
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    throw new IllegalStateException("never");
  }

  @Override
  public String getDisplayString(String[] children) {
    return "current_database()";
  }
}
