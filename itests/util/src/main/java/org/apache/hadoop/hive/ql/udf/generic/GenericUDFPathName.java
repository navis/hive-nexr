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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

@Description(name = "pathname",
    value = "_FUNC_(n0) - Returns the final component of input path")
public class GenericUDFPathName extends GenericUDF {

  StringObjectInspector inputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1 || !(arguments[0] instanceof StringObjectInspector)) {
      throw new UDFArgumentException("pathname accepts one string input");
    }
    inputOI = (StringObjectInspector) arguments[0];
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    if (inputOI.preferWritable()) {
      Text text = inputOI.getPrimitiveWritableObject(arguments[0].get());
      if (text == null) {
        return null;
      }
      byte[] bytes = text.getBytes();
      int i = text.getLength() - 1;
      for (; i >= 0; i--) {
        if (bytes[i] == Path.SEPARATOR_CHAR) {
          text.set(bytes, i + 1, text.getLength() - (i + 1));
          break;
        }
      }
      return text;
    }
    String string = inputOI.getPrimitiveJavaObject(arguments[0].get());
    if (string == null) {
      return null;
    }
    int index = string.lastIndexOf(Path.SEPARATOR_CHAR);
    if (index >= 0) {
      return string.substring(index + 1, string.length());
    }
    return string;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("pathname(");
    for (int i = 0; i < children.length; i++) {
      sb.append(children[i]);
      if (i + 1 != children.length) {
        sb.append(",");
      }
    }
    sb.append(")");
    return sb.toString();
  }
}
