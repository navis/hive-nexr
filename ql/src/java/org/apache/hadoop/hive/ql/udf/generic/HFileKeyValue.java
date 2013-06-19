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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

@Description(name = "hfile_kv",
    value = "_FUNC_(a,b,c..,'hbase column mapping') - makes key value pair for each values")
    public class HFileKeyValue extends GenericUDTF {

  private int keyIndex;
  private byte[] tags;
  private final StandardUnion union = new StandardUnion();
  private final Object[] forward = new Object[] { null, union };

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    if (argOIs.length < 3) {
      throw new UDFArgumentException("hfile_union should have at least three arguments");
    }
    ObjectInspector mapOI = argOIs[argOIs.length - 1];
    if (!(mapOI instanceof ConstantObjectInspector && mapOI instanceof StringObjectInspector)) {
      throw new UDFArgumentException("The last argument should be a constant string");
    }
    String mapping = String.valueOf(((ConstantObjectInspector) mapOI).getWritableConstantValue());
    String[] splits = mapping.split(",");
    if (splits.length != argOIs.length - 1) {
      throw new UDFArgumentException("The number of columns is not matching " +
          "with column mapping information");
    }
    int numValues = splits.length - 1;

    ColumnIndex[] valCols = new ColumnIndex[numValues];
    ObjectInspector[] valOIs = new ObjectInspector[numValues];

    byte tag = 0;
    for (int i = 0; i < argOIs.length - 1; i++) {
      int sharp = splits[i].indexOf('#');
      if (sharp > 0) {
        splits[i] = splits[i].substring(0, sharp);
      }
      if (splits[i].equals(":key")) {
        keyIndex = i;
        continue;
      }
      valOIs[tag] = argOIs[i];
      valCols[tag] = new ColumnIndex(splits[i], tag++);
    }
    Arrays.sort(valCols);
    byte[] tags = new byte[valCols.length];
    for (int i = 0; i < tags.length; i++) {
      tags[i] = valCols[i].tag;
    }
    this.tags = tags;

    ObjectInspector unionOI = ObjectInspectorFactory.getStandardUnionObjectInspector(
        Arrays.asList(valOIs));
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList("key", "values"), Arrays.asList(argOIs[keyIndex], unionOI));
  }

  @Override
  public void process(Object[] args) throws HiveException {
    forward[0] = args[keyIndex];
    for (int i = 0; i < tags.length; i++) {
      union.setObject(args[i]);
      union.setTag(tags[i]);
      forward(forward);
    }
  }

  @Override
  public void close() throws HiveException {
  }

  private static class ColumnIndex implements Comparable<ColumnIndex> {

    private final String columnName;
    private final byte tag;

    public ColumnIndex(String columnName, byte tag) {
      this.columnName = columnName;
      this.tag = tag;
    }

    public int compareTo(ColumnIndex o) {
      return columnName.compareTo(o.columnName);
    }
  }
}
