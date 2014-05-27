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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * GenericUDF Class for SQL construct "NULLIF(a, b)".
 */
@Description(name = "nullif",
    value = "_FUNC_(a1, a2) - Returns null if two expressions are equivalent. Returns first argument if it's not",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(1, 1) FROM src LIMIT 1;\n" + "null")
public class GenericUDFNullIf extends GenericUDF {

  private transient ObjectInspector oi1;
  private transient ObjectInspector oi2;

  private transient Object constant;
  private transient boolean useConstant;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException("nullif takes two arguments");
    }
    oi1 = arguments[0];
    oi2 = arguments[1];

    // If we return constant object inspector, hive will collapse whole UDF into a constant
    if (ObjectInspectorUtils.isConstantObjectInspector(oi1)) {
      Object value = ((ConstantObjectInspector)oi1).getWritableConstantValue();
      ObjectInspector standard = ObjectInspectorUtils.getStandardObjectInspector(oi1);
      constant = ObjectInspectorUtils.copyToStandardObject(value, standard);
      useConstant = true;
      return standard;
    }
    return oi1;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object v1 = arguments[0].get();
    if (v1 == null) {
      return useConstant ? constant : v1;
    }
    Object v2 = arguments[1].get();
    if (v2 != null && ObjectInspectorUtils.compare(v1, oi1, v2, oi2) == 0) {
      return null;
    }
    return useConstant ? constant : v1;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("NULLIF(");
    if (children.length > 0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(",");
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }

}
