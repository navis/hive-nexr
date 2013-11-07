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

package org.apache.hive.service.cli;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TRowSet;

/**
 * RowSet.
 *
 */
public class RowSet implements Iterable<Object[]> {

  private long startOffset;

  private final List<Type> types;
  private final List<ColumnValue> values;

  public RowSet(TableSchema schema) {
    types = new ArrayList<Type>();
    values = new ArrayList<ColumnValue>();
    for (ColumnDescriptor colDesc : schema.getColumnDescriptors()) {
      types.add(colDesc.getType());
      values.add(new ColumnValue(colDesc.getType()));
    }
  }

  public RowSet(List<Type> types, List<ColumnValue> values, long startOffset) {
    this.types = types;
    this.values = values;
    this.startOffset = startOffset;
  }

  public RowSet(TRowSet tRowSet) {
    startOffset = tRowSet.getStartRowOffset();
    types = new ArrayList<Type>();
    values = new ArrayList<ColumnValue>();
    for (TColumnValue tvalue : tRowSet.getColVals()) {
      ColumnValue value = new ColumnValue(tvalue);
      values.add(value);
      types.add(value.getType());
    }
  }

  public RowSet addRow(Object[] fields) {
    for (int i = 0; i < fields.length; i++) {
      values.get(i).addValue(types.get(i), fields[i]);
    }
    return this;
  }

  public List<ColumnValue> getValues() {
    return values;
  }

  public int numColumns() {
    return values.size();
  }

  public int numRows() {
    return values.isEmpty() ? 0 : values.get(0).size();
  }

  public static Object evaluate(ColumnDescriptor desc, Object value) {
    return evaluate(desc.getType(), value);
  }

  public static Object evaluate(Type type, Object value) {
    switch (type) {
      case BOOLEAN_TYPE:
      case TINYINT_TYPE:
      case SMALLINT_TYPE:
      case INT_TYPE:
      case BIGINT_TYPE:
      case FLOAT_TYPE:
      case DOUBLE_TYPE:
      case STRING_TYPE:
        return value;
      case BINARY_TYPE:
        return value == null ? null : ((ByteBuffer) value).array();
      case TIMESTAMP_TYPE:
        return value == null ? null : Timestamp.valueOf((String) value);
      case DECIMAL_TYPE:
        return value == null ? null : new BigDecimal((String)value);
      case VOID_TYPE:
        return null;
      default:
        throw new IllegalArgumentException("Unrecognized column type:" + type);
    }
  }

  public RowSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);

    List<ColumnValue> subset = new ArrayList<ColumnValue>();
    for (int i = 0; i < values.size(); i++) {
      subset.add(values.get(i).extractSubset(0, numRows));
    }
    RowSet result = new RowSet(types, subset, startOffset);
    startOffset += numRows;
    return result;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public RowSet setStartOffset(long startOffset) {
    this.startOffset = startOffset;
    return this;
  }

  public TRowSet toTRowSet() {
    TRowSet tRowSet = new TRowSet(startOffset, new ArrayList<TColumnValue>());
    for (int i = 0; i < values.size(); i++) {
      tRowSet.addToColVals(values.get(i).toTColumnValue());
    }
    return tRowSet;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return new Iterator<Object[]>() {

      private int index;
      private final Object[] convey = new Object[numColumns()];

      @Override
      public boolean hasNext() {
        return index < numRows();
      }

      @Override
      public Object[] next() {
        for (int i = 0; i < values.size(); i++) {
          convey[i] = values.get(i).get(index);
        }
        index++;
        return convey;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public Object[] fill(int index, Object[] convey) {
    for (int i = 0; i < values.size(); i++) {
      convey[i] = values.get(i).get(index);
    }
    return convey;
  }
}
