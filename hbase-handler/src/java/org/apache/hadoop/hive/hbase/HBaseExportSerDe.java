/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.io.Writable;

public class HBaseExportSerDe extends AbstractSerDe {

  private HBaseSerDe serde;
  private HBaseSerDeParameters serdeParam;
  private HBaseUnionWritable dummy;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    serde = new HBaseSerDe();
    serde.initialize(conf, tbl);
    serdeParam = serde.getHBaseSerdeParam();
    dummy = new HBaseUnionWritable(serde.getSerializer(), serdeParam.getKeyIndex());
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return HBaseUnionWritable.class;
  }

  StructObjectInspector inputOI;
  StructField[] fields;

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (inputOI == null) {
      inputOI = (StructObjectInspector) objInspector;
      fields = inputOI.getAllStructFieldRefs().toArray(new StructField[0]);
      dummy.rowKeyField = fields[0];
      dummy.unionOI = (UnionObjectInspector) fields[1].getFieldObjectInspector();
    }
    dummy.rowKey = inputOI.getStructFieldData(obj, fields[0]);
    dummy.union = inputOI.getStructFieldData(obj, fields[1]);
    return dummy;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return serde.getSerDeStats();
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return serde.deserialize(blob);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    int iKey = serdeParam.getKeyIndex();
    StructObjectInspector output = (StructObjectInspector) serde.getObjectInspector();
    List<? extends StructField> fields = output.getAllStructFieldRefs();
    List<ObjectInspector> unionOIs = new ArrayList<ObjectInspector>();
    for (int i = 0; i < fields.size(); i++) {
      if (i != iKey) {
        unionOIs.add(fields.get(i).getFieldObjectInspector());
      }
    }
    ObjectInspector unionOI = ObjectInspectorFactory.getStandardUnionObjectInspector(unionOIs);
    return ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList("key", "values"),
        Arrays.asList(fields.get(iKey).getFieldObjectInspector(), unionOI));

  }

  public static class HBaseUnionWritable implements Writable {

    private Object rowKey;
    private StructField rowKeyField;

    private Object union;
    private UnionObjectInspector unionOI;

    private final HBaseRowSerializer serializer;
    private final int iKey;
    private final byte[][] holder = new byte[3][];  // family | qualifier | data

    public HBaseUnionWritable(HBaseRowSerializer serializer, int iKey) {
      this.serializer = serializer;
      this.iKey = iKey;
    }

    public void write(DataOutput out) throws IOException {
      throw new UnsupportedOperationException("write");
    }

    public void readFields(DataInput in) throws IOException {
      throw new UnsupportedOperationException("readFields");
    }

    public Object getField() {
      return unionOI.getField(union);
    }

    public byte getTag() {
      return unionOI.getTag(union);
    }

    private int getColIndex() {
      byte tag = getTag();
      return tag < iKey ? tag : tag + 1;
    }

    public KeyValue toKeyValue() throws IOException {
      byte[] key = serializer.serializeKey(rowKey, rowKeyField);
      ObjectInspector fieldOI = unionOI.getObjectInspectors().get(getTag());
      serializer.serializeField(getColIndex(), getField(), fieldOI, holder);
      return new KeyValue(key, holder[0], holder[1], holder[2]);
    }
  }
}
