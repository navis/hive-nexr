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

package org.apache.hadoop.hive.serde2.lazybinary;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class LazyBinaryUnion extends LazyBinaryNonPrimitive<LazyBinaryUnionObjectInspector> {

  private byte tag = -1;
  private boolean initialized;
  private LazyBinaryObject field;

  public LazyBinaryUnion(LazyBinaryUnionObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    initialized = false;
    tag = -1;
  }

  public byte getTag() {
    if (tag == -1) {
      parse();
    }
    return tag;
  }

  public Object getField() {
    if (tag == -1) {
      parse();
    }
    if (!initialized && field != null) {
      field.init(bytes, start + 1, length - 1);
    }
    initialized = true;
    return field == null ? null : field.getObject();
  }

  private void parse() {
    byte[] data = bytes.getData();
    ObjectInspector fieldOI = oi.getObjectInspectors().get(tag = data[start]);
    field = LazyBinaryFactory.createLazyBinaryObject(fieldOI);
  }
}
