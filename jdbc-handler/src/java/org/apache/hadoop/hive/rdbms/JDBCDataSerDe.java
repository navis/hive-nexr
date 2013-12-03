package org.apache.hadoop.hive.rdbms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

public class JDBCDataSerDe implements SerDe {

  private final RowWritable cachedWritable = new RowWritable();

  private StructObjectInspector objectInspector;

  public void initialize(Configuration entries, Properties properties) throws SerDeException {

    String columns = properties.getProperty(ConfigurationUtils.LIST_COLUMNS);
    String types = properties.getProperty(ConfigurationUtils.LIST_COLUMN_TYPES);

    List<String> columnNames = Arrays.asList(columns.split(","));
    PrimitiveCategory[] columnTypes = ConfigurationUtils.toTypes(types.split(":"));
    PrimitiveObjectInspector[] columnOIsArray = ConfigurationUtils.toPrimitiveJavaOIs(columnTypes);

    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames,
        new ArrayList<ObjectInspector>(Arrays.asList(columnOIsArray)));
  }

  public Class<? extends Writable> getSerializedClass() {
    return RowWritable.class;
  }

  public Writable serialize(Object obj, ObjectInspector objectInspector) throws SerDeException {
    if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new SerDeException(getClass().toString() + " can only serialize struct types, " +
          "but we got: " + objectInspector.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector rowOI = (StructObjectInspector) objectInspector;

    List<? extends StructField> fields = rowOI.getAllStructFieldRefs();
    cachedWritable.clear();
    for (StructField field : fields) {
      PrimitiveObjectInspector fOI = (PrimitiveObjectInspector) field.getFieldObjectInspector();
      cachedWritable.add(fOI.getPrimitiveJavaObject(rowOI.getStructFieldData(obj, field)));
    }
    return cachedWritable;
  }

  public List<Object> deserialize(Writable writable) throws SerDeException {
    if (!(writable instanceof RowWritable)) {
      throw new SerDeException("Expected RowWritable, but received: " + writable.getClass().getName());
    }
    return ((RowWritable) writable).get();
  }

  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  public SerDeStats getSerDeStats() {
    return null;
  }

}
