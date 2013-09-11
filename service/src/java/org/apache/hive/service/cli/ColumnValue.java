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
import java.sql.Timestamp;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hive.service.cli.thrift.TBinaryValue;
import org.apache.hive.service.cli.thrift.TBoolValue;
import org.apache.hive.service.cli.thrift.TByteValue;
import org.apache.hive.service.cli.thrift.TColumnValue;
import org.apache.hive.service.cli.thrift.TDoubleValue;
import org.apache.hive.service.cli.thrift.TI16Value;
import org.apache.hive.service.cli.thrift.TI32Value;
import org.apache.hive.service.cli.thrift.TI64Value;
import org.apache.hive.service.cli.thrift.TStringValue;

/**
 * ColumnValue.
 *
 */
public class ColumnValue {

  public static final TColumnValue NULL = new TColumnValue();

  static {
    NULL.setStringVal(new TStringValue());
  }

  // TODO: replace this with a non-Thrift implementation
  private final TColumnValue tColumnValue;

  public ColumnValue(TColumnValue tColumnValue) {
    this.tColumnValue = new TColumnValue(tColumnValue);
  }

  private static boolean isNull(Object value) {
    return (value == null);
  }

  public static ColumnValue booleanValue(Boolean value) {
    TBoolValue tBoolValue = new TBoolValue();
    if (value != null) {
      tBoolValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.boolVal(tBoolValue));
  }

  public static ColumnValue byteValue(Byte value) {
    TByteValue tByteValue = new TByteValue();
    if (value != null) {
      tByteValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.byteVal(tByteValue));
  }

  public static ColumnValue shortValue(Short value) {
    TI16Value tI16Value = new TI16Value();
    if (value != null) {
      tI16Value.setValue(value);
    }
    return new ColumnValue(TColumnValue.i16Val(tI16Value));
  }

  public static ColumnValue intValue(Integer value) {
    TI32Value tI32Value = new TI32Value();
    if (value != null) {
      tI32Value.setValue(value);
    }
    return new ColumnValue(TColumnValue.i32Val(tI32Value));
  }

  public static ColumnValue longValue(Long value) {
    TI64Value tI64Value = new TI64Value();
    if (value != null) {
      tI64Value.setValue(value);
    }
    return new ColumnValue(TColumnValue.i64Val(tI64Value));
  }

  public static ColumnValue floatValue(Float value) {
    TDoubleValue tDoubleValue = new TDoubleValue();
    if (value != null) {
      tDoubleValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.doubleVal(tDoubleValue));
  }

  public static ColumnValue doubleValue(Double value) {
    TDoubleValue tDoubleValue = new TDoubleValue();
    if (value != null) {
      tDoubleValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.doubleVal(tDoubleValue));
  }

  public static ColumnValue stringValue(String value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.stringVal(tStringValue));
  }

  public static ColumnValue timestampValue(Timestamp value) {
    TStringValue tStringValue = new TStringValue();
    if (value != null) {
      tStringValue.setValue(value.toString());
    }
    return new ColumnValue(TColumnValue.stringVal(tStringValue));
  }

  public static ColumnValue stringValue(HiveDecimal value) {
    TStringValue tStrValue = new TStringValue();
    if (value != null) {
      tStrValue.setValue(value.toString());
    }
    return new ColumnValue(TColumnValue.stringVal(tStrValue));
  }

  public static ColumnValue binaryValue(byte[] value) {
    TBinaryValue tBinaryValue = new TBinaryValue();
    if (value != null) {
      tBinaryValue.setValue(value);
    }
    return new ColumnValue(TColumnValue.binaryVal(tBinaryValue));
  }

  public static ColumnValue newColumnValue(Type type, Object value) {
    switch (type) {
    case BOOLEAN_TYPE:
      return booleanValue((Boolean)value);
    case TINYINT_TYPE:
      return byteValue((Byte)value);
    case SMALLINT_TYPE:
      return shortValue((Short)value);
    case INT_TYPE:
      return intValue((Integer)value);
    case BIGINT_TYPE:
      return longValue((Long)value);
    case FLOAT_TYPE:
      return floatValue((Float)value);
    case DOUBLE_TYPE:
      return doubleValue((Double)value);
    case STRING_TYPE:
      return stringValue((String)value);
    case TIMESTAMP_TYPE:
      return timestampValue((Timestamp)value);
    case DECIMAL_TYPE:
      return stringValue(((HiveDecimal)value));
    case BINARY_TYPE:
      return binaryValue(((byte[]) value));
    case ARRAY_TYPE:
    case MAP_TYPE:
    case STRUCT_TYPE:
    case UNION_TYPE:
    case USER_DEFINED_TYPE:
      return stringValue((String)value);
    default:
      return null;
    }
  }

  public TColumnValue toTColumnValue() {
    return new TColumnValue(tColumnValue);
  }



  private Boolean getBooleanValue(TBoolValue tBoolValue) {
    if (tBoolValue.isSetValue()) {
      return tBoolValue.isValue();
    }
    return null;
  }

  private Byte getByteValue(TByteValue tByteValue) {
    if (tByteValue.isSetValue()) {
      return tByteValue.getValue();
    }
    return null;
  }

  private Short getShortValue(TI16Value tI16Value) {
    if (tI16Value.isSetValue()) {
      return tI16Value.getValue();
    }
    return null;
  }

  private Integer getIntegerValue(TI32Value tI32Value) {
    if (tI32Value.isSetValue()) {
      return tI32Value.getValue();
    }
    return null;
  }

  private Long getLongValue(TI64Value tI64Value) {
    if (tI64Value.isSetValue()) {
      return tI64Value.getValue();
    }
    return null;
  }

  private Double getDoubleValue(TDoubleValue tDoubleValue) {
    if (tDoubleValue.isSetValue()) {
      return tDoubleValue.getValue();
    }
    return null;
  }

  private String getStringValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return tStringValue.getValue();
    }
    return null;
  }

  private Timestamp getTimestampValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return Timestamp.valueOf(tStringValue.getValue());
    }
    return null;
  }

  private byte[] getBinaryValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return tStringValue.getValue().getBytes();
    }
    return null;
  }

  private BigDecimal getDecimalValue(TStringValue tStringValue) {
    if (tStringValue.isSetValue()) {
      return new BigDecimal(tStringValue.getValue());
    }
    return null;
  }

  public Object getColumnValue(Type columnType) throws Exception {
    switch (columnType) {
    case BOOLEAN_TYPE:
      return getBooleanValue(tColumnValue.getBoolVal());
    case TINYINT_TYPE:
      return getByteValue(tColumnValue.getByteVal());
    case SMALLINT_TYPE:
      return getShortValue(tColumnValue.getI16Val());
    case INT_TYPE:
      return getIntegerValue(tColumnValue.getI32Val());
    case BIGINT_TYPE:
      return getLongValue(tColumnValue.getI64Val());
    case FLOAT_TYPE:
      return getDoubleValue(tColumnValue.getDoubleVal());
    case DOUBLE_TYPE:
      return getDoubleValue(tColumnValue.getDoubleVal());
    case STRING_TYPE:
      return getStringValue(tColumnValue.getStringVal());
    case TIMESTAMP_TYPE:
      return getTimestampValue(tColumnValue.getStringVal());
    case BINARY_TYPE:
      return getBinaryValue(tColumnValue.getStringVal());
    case DECIMAL_TYPE:
      return getDecimalValue(tColumnValue.getStringVal());
    default:
      if (tColumnValue.getSetField() == TColumnValue._Fields.STRING_VAL) {
        TStringValue string = tColumnValue.getStringVal();
        return string.isSetValue() ? string.getValue() : null;
      }
      throw new IllegalArgumentException("Unrecognized column type:" + columnType);
    }
  }

  public String toString() {
    if (tColumnValue.isSetBoolVal()) {
      TBoolValue boolval = tColumnValue.getBoolVal();
      return boolval.isSetValue() ? String.valueOf(boolval.isValue()) : null;
    }
    if (tColumnValue.isSetByteVal()) {
      TByteValue byteval = tColumnValue.getByteVal();
      return byteval.isSetValue() ? String.valueOf(byteval.getValue()) : null;
    }
    if (tColumnValue.isSetI16Val()) {
      TI16Value i16val = tColumnValue.getI16Val();
      return i16val.isSetValue() ? String.valueOf(i16val.getValue()) : null;
    }
    if (tColumnValue.isSetI32Val()) {
      TI32Value i32val = tColumnValue.getI32Val();
      return i32val.isSetValue() ? String.valueOf(i32val.getValue()) : null;
    }
    if (tColumnValue.isSetI64Val()) {
      TI64Value i64val = tColumnValue.getI64Val();
      return i64val.isSetValue() ? String.valueOf(i64val.getValue()) : null;
    }
    if (tColumnValue.isSetDoubleVal()) {
      TDoubleValue doubleval = tColumnValue.getDoubleVal();
      return doubleval.isSetValue() ? String.valueOf(doubleval.getValue()) : null;
    }
    if (tColumnValue.isSetStringVal()) {
      TStringValue strval = tColumnValue.getStringVal();
      return strval.isSetValue() ? strval.getValue() : null;
    }
    return null;
  }
}
