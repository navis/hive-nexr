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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.util.MorphlineProcessor;

public class GenericMorphlineUDF extends GenericUDF {

  private transient ObjectInspectorConverters.Converter[] converters;
  private transient Object[] inputFields;
  private transient MorphlineProcessor processor;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "At least morphline definition and properties should be provided");
    }
    for (int i = 0; i < 1; i++) {
      if (!(arguments[i] instanceof WritableConstantStringObjectInspector)) {
        throw new UDFArgumentTypeException(i, "Morphline arguments shoud be constant string");
      }
    }
    converters = new ObjectInspectorConverters.Converter[arguments.length - 2];
    for (int i = 0; i < converters.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i + 2],
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    inputFields = new Object[converters.length];

    String morphlineDef =
        ((WritableConstantStringObjectInspector)arguments[0]).getWritableConstantValue().toString();
    String morphlineConf =
        ((WritableConstantStringObjectInspector)arguments[1]).getWritableConstantValue().toString();

    Config[] override = new Config[] {};
    if (morphlineConf.length() > 0) {
      override = new Config[] {ConfigFactory.parseString(morphlineConf)};
    }
    try {
      processor = MorphlineProcessor.build(morphlineDef, override);
    } catch (Exception e) {
      throw new UDFArgumentException(e);
    }
    return processor.getOutputOI();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length - 2; i++) {
      inputFields[i] = converters[i].convert(arguments[i + 2].get());
    }
    return processor.push(inputFields);
  }

  @Override
  public String getDisplayString(String[] children) {
    return "morphline";
  }
}
