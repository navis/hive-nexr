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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ResultWrapper extends Result {

  private KeyValue[] target;
  private NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap;

  public void setTarget(List<KeyValue> kvs) {
    target = kvs.toArray(new KeyValue[kvs.size()]);
    familyMap = null;
  }

  @Override
  public byte[] getRow() {
    return isEmpty() ? null : target[0].getRow();
  }

  @Override
  public KeyValue[] raw() {
    return target;
  }

  @Override
  public List<KeyValue> list() {
    return Arrays.asList(target);
  }

  @Override
  public byte[] value() {
    return isEmpty() ? null : target[0].getValue();
  }

  @Override
  public boolean isEmpty() {
    return target == null || target.length == 0;
  }

  @Override
  public int size() {
    return target == null ? 0 : target.length;
  }

  @Override
  public NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> getMap() {
    if(this.familyMap != null) {
      return this.familyMap;
    }
    if(isEmpty()) {
      return null;
    }
    this.familyMap =
      new TreeMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      (Bytes.BYTES_COMPARATOR);
    for(KeyValue kv : this.target) {
      KeyValue.SplitKeyValue splitKV = kv.split();
      byte [] family = splitKV.getFamily();
      NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap =
        familyMap.get(family);
      if(columnMap == null) {
        columnMap = new TreeMap<byte[], NavigableMap<Long, byte[]>>
          (Bytes.BYTES_COMPARATOR);
        familyMap.put(family, columnMap);
      }
      byte [] qualifier = splitKV.getQualifier();
      NavigableMap<Long, byte[]> versionMap = columnMap.get(qualifier);
      if(versionMap == null) {
        versionMap = new TreeMap<Long, byte[]>(new Comparator<Long>() {
          public int compare(Long l1, Long l2) {
            return l2.compareTo(l1);
          }
        });
        columnMap.put(qualifier, versionMap);
      }
      Long timestamp = Bytes.toLong(splitKV.getTimestamp());
      byte [] value = splitKV.getValue();
      versionMap.put(timestamp, value);
    }
    return this.familyMap;
  }

  @Override
  public NavigableMap<byte[], byte[]> getFamilyMap(byte [] family) {
    if(this.familyMap == null) {
      getMap();
    }
    if(isEmpty()) {
      return null;
    }
    NavigableMap<byte[], byte[]> returnMap =
      new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifierMap =
      familyMap.get(family);
    if(qualifierMap == null) {
      return returnMap;
    }
    for(Map.Entry<byte[], NavigableMap<Long, byte[]>> entry :
      qualifierMap.entrySet()) {
      byte [] value =
        entry.getValue().get(entry.getValue().firstKey());
      returnMap.put(entry.getKey(), value);
    }
    return returnMap;
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    throw new UnsupportedOperationException("readFields");
  }

  @Override
  public long getWritableSize() {
    throw new UnsupportedOperationException("getWritableSize");
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    throw new UnsupportedOperationException("write");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("keyvalues=");
    if (isEmpty()) {
      sb.append("NONE");
      return sb.toString();
    }
    sb.append("{");
    boolean moreThanOne = false;
    for (KeyValue kv : target) {
      if (moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append(kv.toString());
    }
    sb.append("}");
    return sb.toString();
  }
}
