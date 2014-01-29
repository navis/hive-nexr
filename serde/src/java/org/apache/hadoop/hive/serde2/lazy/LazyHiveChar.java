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

import java.nio.charset.CharacterCodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.io.Text;

/**
 * LazyObject for storing a value of HiveChar.
 *
 */
public class LazyHiveChar extends
    LazyPrimitive<LazyHiveCharObjectInspector, HiveCharWritable> {

  private static final Log LOG = LogFactory.getLog(LazyHiveChar.class);

  protected int maxLength = -1;
  protected transient boolean warned;

  public LazyHiveChar(LazyHiveCharObjectInspector oi) {
    super(oi);
    maxLength = ((CharTypeInfo)oi.getTypeInfo()).getLength();
    data = new HiveCharWritable();
  }

  public LazyHiveChar(LazyHiveChar copy) {
    super(copy);
    this.maxLength = copy.maxLength;
    data = new HiveCharWritable(copy.data);
  }

  public void setValue(LazyHiveChar copy) {
    data.set(copy.data, maxLength);
  }

  @Override
  public void init(byte[] bytes, int start, int length) {
    if (oi.isEscaped()) {
      Text textData =  data.getTextValue();
      // This is doing a lot of copying here, this could be improved by enforcing length
      // at the same time as escaping rather than as separate steps.
      LazyUtils.copyAndEscapeStringDataToText(bytes, start, length,
          oi.getEscapeChar(), textData);
      data.set(textData.toString(), maxLength);
      isNull = false;
    } else {
      try {
        String byteData = Text.decode(bytes, start, length);
        data.set(byteData, maxLength);
        isNull = false;
      } catch (CharacterCodingException e) {
        isNull = true;
        if (!warned) {
          warned = true;
          LOG.info("Data not in the HiveChar data type range so converted to null. " +
              "Similar problems will not be logged further.", e);
        }
      }
    }
  }

}
