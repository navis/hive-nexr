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
  private final List<ColumnValue> values;

  public RowSet(TableSchema schema) {
    values = new ArrayList<ColumnValue>(schema.getSize());
    for (ColumnDescriptor colDesc : schema.getColumnDescs()) {
      values.add(new ColumnValue(colDesc.getType()));
    }
  }

  private RowSet(List<ColumnValue> values, long startOffset) {
    this.values = values;
    this.startOffset = startOffset;
  }

  public RowSet(TRowSet tRowSet) {
    startOffset = tRowSet.getStartRowOffset();
    values = new ArrayList<ColumnValue>();
    for (TColumnValue tvalue : tRowSet.getColVals()) {
      values.add(new ColumnValue(tvalue));
    }
  }

  public RowSet addRow(Object[] fields) {
    for (int i = 0; i < fields.length; i++) {
      values.get(i).addValue(fields[i]);
    }
    return this;
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
    try {
      switch (type) {
        case BOOLEAN_TYPE:
        case TINYINT_TYPE:
        case SMALLINT_TYPE:
        case INT_TYPE:
        case BIGINT_TYPE:
        case DOUBLE_TYPE:
        case STRING_TYPE:
          return value;
        case FLOAT_TYPE:
          return value == null ? null : ((Double) value).floatValue();
        case BINARY_TYPE:
          return value == null ? null : ((ByteBuffer) value).array();
        case TIMESTAMP_TYPE:
          return value == null ? null : Timestamp.valueOf((String) value);
        case DECIMAL_TYPE:
          return value == null ? null : new BigDecimal((String)value);
        case VOID_TYPE:
          return null;
        case ARRAY_TYPE:
        case MAP_TYPE:
        case STRUCT_TYPE:
          // todo: returns json string. should recreate object from it?
          return value;
        default:
          throw new IllegalArgumentException("Unrecognized column type:" + type);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Faild to evaluate " + value + " to " + type, e);
    }
  }

  public RowSet extractSubset(int maxRows) {
    int numRows = Math.min(numRows(), maxRows);

    List<ColumnValue> subset = new ArrayList<ColumnValue>();
    for (int i = 0; i < values.size(); i++) {
      subset.add(values.get(i).extractSubset(0, numRows));
    }
    RowSet result = new RowSet(subset, startOffset);
    startOffset += numRows;
    return result;
  }

  public TRowSet toTRowSet() {
    List<TColumnValue> colVals = new ArrayList<TColumnValue>(numColumns());
    for (int i = 0; i < values.size(); i++) {
      colVals.add(values.get(i).toTColumnValue());
    }
    return new TRowSet(startOffset, colVals);
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
}
