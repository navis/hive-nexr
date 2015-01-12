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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
import org.apache.hadoop.hive.serde2.StructObject;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * LazyObject for storing a struct. The field of a struct can be primitive or
 * non-primitive.
 *
 * LazyStruct does not deal with the case of a NULL struct. That is handled by
 * the parent LazyObject.
 */
public class LazyStruct extends LazyNonPrimitive<LazySimpleStructObjectInspector>
    implements StructObject, SerDeStatsStruct {

  private static Log LOG = LogFactory.getLog(LazyStruct.class.getName());

  /**
   * Whether the data is already parsed or not.
   */
  boolean parsed;

  /**
   * Size of serialized data
   */
  long serializedSize;

  /**
   * The start positions of struct fields. Only valid when the data is parsed.
   * Note that startPosition[arrayLength] = begin + length + 1; that makes sure
   * we can use the same formula to compute the length of each element of the
   * array.
   *
   * note updated to use separate arrays for start and end position to allow for trim
   */
  int[] fieldStart;
  int[] fieldLength;

  /**
   * The fields of the struct.
   */
  LazyObjectBase[] fields;
  /**
   * Whether init() has been called on the field or not.
   */
  boolean[] fieldInited;

  /**
   * Construct a LazyStruct object with the ObjectInspector.
   */
  public LazyStruct(LazySimpleStructObjectInspector oi) {
    super(oi);
  }

  /**
   * Set the row data for this LazyStruct.
   *
   * @see LazyObject#init(ByteArrayRef, int, int)
   */
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
    serializedSize = length;
  }

  boolean missingFieldWarned = false;
  boolean extraFieldWarned = false;

  /**
   * Parse the byte[] and fill each field.
   */
  private void parse() {

    byte[] separator = oi.getSeparator();
    boolean lastColumnTakesRest = oi.getLastColumnTakesRest();
    boolean isEscaped = oi.isEscaped();
    byte escapeChar = oi.getEscapeChar();

    if (fields == null) {
      initLazyFields(oi.getAllStructFieldRefs());
    }

    final int structByteEnd = start + length;
    
    final byte[] data = bytes.getData();

    // Go through all bytes in the byte[]
    
    Arrays.fill(fieldStart, -1);
    Arrays.fill(fieldLength, -1);

    int fieldId = 0;
    int index = start;
    
    fieldStart[0] = index;
    while (index <= structByteEnd) {
      if (index == structByteEnd || isDelimiter(data, index, separator)) {
        if (fieldId == fields.length - 1) {
          fieldLength[fieldId] = 
              (lastColumnTakesRest ? structByteEnd : index) - fieldStart[fieldId];
          break;
        }
        fieldLength[fieldId] = index - fieldStart[fieldId];
        fieldStart[++fieldId] = index += separator.length;
      } else {
        if (isEscaped && data[index] == escapeChar) {
          // ignore the char after escape_char
          index += 2;
        } else {
          index++;
        }
      }
    }

    // Extra bytes at the end?
    if (!extraFieldWarned && index < structByteEnd) {
      extraFieldWarned = true;
      LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
          + "problems.");
    }

    // Missing fields?
    if (!missingFieldWarned && fieldId < fields.length) {
      missingFieldWarned = true;
      LOG.info("Missing fields! Expected " + fields.length + " fields but "
          + "only got " + fieldId + "! Ignoring similar problems.");
    }

    Arrays.fill(fieldInited, false);
    parsed = true;
  }

  protected final void initLazyFields(List<? extends StructField> fieldRefs) {
    fields = new LazyObjectBase[fieldRefs.size()];
    for (int i = 0; i < fields.length; i++) {
      try {
        fields[i] = createLazyField(i, fieldRefs.get(i));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    fieldInited = new boolean[fields.length];
    fieldStart = new int[fields.length];
    fieldLength = new int[fields.length];
  }

  protected LazyObjectBase createLazyField(int fieldID, StructField fieldRef) throws SerDeException {
    return LazyFactory.createLazyObject(fieldRef.getFieldObjectInspector());
  }

  /**
   * Get one field out of the struct.
   *
   * If the field is a primitive field, return the actual object. Otherwise
   * return the LazyObject. This is because PrimitiveObjectInspector does not
   * have control over the object used by the user - the user simply directly
   * use the Object instead of going through Object
   * PrimitiveObjectInspector.get(Object).
   *
   * @param fieldID
   *          The field ID
   * @return The field as a LazyObject
   */
  public Object getField(int fieldID) {
    if (!parsed) {
      parse();
    }
    return uncheckedGetField(fieldID);
  }

  /**
   * Get the field out of the row without checking parsed. This is called by
   * both getField and getFieldsAsList.
   *
   * @param fieldID
   *          The id of the field starting from 0.
   * @param nullSequence
   *          The sequence representing NULL value.
   * @return The value of the field
   */
  private Object uncheckedGetField(int fieldID) {
    if (fieldInited[fieldID]) {
      return fields[fieldID].getObject();
    }
    fieldInited[fieldID] = true;

    if (isNull(oi.getNullSequence(), bytes, fieldStart[fieldID], fieldLength[fieldID])) {
      fields[fieldID].setNull();
    } else {
      fields[fieldID].init(bytes, fieldStart[fieldID], fieldLength[fieldID]);
    }
    return fields[fieldID].getObject();
  }

  private transient List<Object> cachedList;

  /**
   * Get the values of the fields as an ArrayList.
   *
   * @return The values of the fields as an ArrayList.
   */
  public List<Object> getFieldsAsList() {
    if (!parsed) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < fields.length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  protected boolean getParsed() {
    return parsed;
  }

  protected void setParsed(boolean parsed) {
    this.parsed = parsed;
  }

  protected LazyObjectBase[] getFields() {
    return fields;
  }

  protected void setFields(LazyObject[] fields) {
    this.fields = fields;
  }

  protected boolean[] getFieldInited() {
    return fieldInited;
  }

  protected void setFieldInited(boolean[] fieldInited) {
    this.fieldInited = fieldInited;
  }

  public long getRawDataSerializedSize() {
    return serializedSize;
  }

  private boolean isDelimiter(byte[] data, int index, byte[] delimiter) {
    if (data.length < index + delimiter.length) {
      return false;
    }
    for (int i = 0; i < delimiter.length; i++) {
      if (data[index + i] != delimiter[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return the data in bytes corresponding to this given struct. This is useful specifically in
   * cases where the data is stored in serialized formats like protobufs or thrift and would need
   * custom deserializers to be deserialized.
   * */
  public byte[] getBytes() {
    return bytes.getData();
  }
}
