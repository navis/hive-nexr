package org.apache.hadoop.hive.hbase;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.hbase.struct.HBaseStructValue;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;

/**
 * Test specific implementation of {@link org.apache.hadoop.hive.serde2.lazy.LazyStruct}
 */
public class HBaseTestStructSerializer extends HBaseStructValue {

  protected String bytesAsString;
  protected Properties tbl;
  protected Configuration conf;
  protected ColumnMapping colMapping;
  protected String testValue;

  public HBaseTestStructSerializer(LazySimpleStructObjectInspector oi, Properties tbl,
      Configuration conf, ColumnMapping colMapping) {
    super(oi);
    this.tbl = tbl;
    this.conf = conf;
    this.colMapping = colMapping;
  }

  @Override
  public void init(byte[] bytes, int start, int length) {
    this.bytes = bytes;
  }

  @Override
  public Object getField(int fieldID) {
    if (bytesAsString == null) {
      bytesAsString = Bytes.toString(bytes).trim();
    }

    // Randomly pick the character corresponding to the field id and convert it to byte array
    byte[] fieldBytes = new byte[] { (byte) bytesAsString.charAt(fieldID) };

    return toLazyObject(fieldID, fieldBytes);
  }
}