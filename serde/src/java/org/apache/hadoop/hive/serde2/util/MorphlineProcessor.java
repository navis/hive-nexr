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

package org.apache.hadoop.hive.serde2.util;

import com.typesafe.config.Config;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;

import java.io.File;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class MorphlineProcessor {

  public static final String PARSE_ONLY = "parseOnly";

  // in script schema
  public static final String INPUT_FIELDS = "inputFields";
  public static final String OUTPUT_FIELDS = "outputFields";

  private final String[] inputFields;
  private final ObjectInspector outputOI;

  private final Command command;
  private final OutputSink outputSink;

  private final Config morphConf;

  public static MorphlineProcessor build(
      String morphlineDef, Config... overrides) throws Exception {
    return new MorphlineProcessor(morphlineDef, null, null, overrides);
  }

  public static MorphlineProcessor build(
      String morphlineDef, String[] columnNames, TypeInfo[] columnTypes, Config... overrides)
      throws Exception {
    return new MorphlineProcessor(morphlineDef, columnNames, columnTypes, overrides);
  }

  private MorphlineProcessor(
      String morphlineDef, String[] columnNames, TypeInfo[] columnTypes, Config... overrides)
      throws Exception {
    String morphlineFile = morphlineDef;
    String morphlineID = null;
    int index = morphlineDef.indexOf('#');
    if (index >= 0) {
      morphlineFile = morphlineDef.substring(0, index);
      morphlineID = morphlineDef.substring(index + 1);
    }

    MorphlineContext context = new MorphlineContext.Builder().build();
    org.kitesdk.morphline.base.Compiler compiler = new org.kitesdk.morphline.base.Compiler();
    Config config = compiler.parse(new File(morphlineFile), overrides);
    morphConf = compiler.find(morphlineID, config, morphlineFile);
    inputFields = config.getStringList(INPUT_FIELDS).toArray(new String[0]);

    if (!config.hasPath(OUTPUT_FIELDS) ||
        (config.hasPath(PARSE_ONLY) && config.getBoolean(PARSE_ONLY))) {
      outputOI = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
      command = compiler.compile(morphConf, context, outputSink = null);
      return;
    }

    List<String> schema = config.getStringList(OUTPUT_FIELDS);

    if (columnNames == null) {
      columnNames = new String[schema.size()];
      columnTypes = new TypeInfo[schema.size()];
      for (int i = 0 ; i < columnNames.length; i++) {
        String column = schema.get(i);
        int x = column.indexOf("#");
        if (x < 0) {
          columnNames[i] = column;
          columnTypes[i] = TypeInfoFactory.stringTypeInfo;
        } else {
          columnNames[i] = column.substring(0, x);
          columnTypes[i] = TypeInfoUtils.getTypeInfoFromTypeString(column.substring(x + 1));
        }
      }
    }
    ObjectInspector[] columnOIs = new ObjectInspector[columnNames.length];
    for (int c = 0 ; c < columnTypes.length; c++) {
      TypeInfo typeInfo = columnTypes[c];
      if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new IllegalArgumentException(
            "Morphline  doesn't allow column " + columnNames[c] + " of type " + typeInfo);
      }
      PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
      columnOIs[c] = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pti);
    }
    outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList(columnNames), Arrays.asList(columnOIs), null);
    outputSink = new OutputSink(columnNames, columnTypes);
    command = compiler.compile(morphConf, context, outputSink);
  }

  public ObjectInspector getOutputOI() {
    return outputOI;
  }

  private final Record record = new Record();

  public Object push(Object... objects) {
    record.getFields().clear();
    for (int i = 0; i < inputFields.length; i++) {
      record.put(inputFields[i], objects[i]);
    }
    if (outputSink == null) {
      return command.process(record);
    }
    return command.process(record) ? outputSink.output : null;
  }

  private static class OutputSink implements Command {

    private final String[] columnNames;
    private final TypeInfo[] columnTypes;
    private final Object[] output;

    public OutputSink(String[] columnNames, TypeInfo[] columnTypes) {
      this.columnNames = columnNames;
      this.columnTypes = columnTypes;
      this.output = new Object[columnNames.length];
    }

    @Override
    public void notify(Record record) {
    }
    @Override
    public boolean process(Record record) {
      for (int c = 0 ; c < columnNames.length; c++) {
        // todo expects string outputs only
        output[c] = convertToType((String)record.getFirstValue(columnNames[c]), columnTypes[c]);
      }
      return true;
    }
    @Override
    public Command getParent() {
      return null;
    }

    private Object convertToType(String t, TypeInfo typeInfo) {
      if (t == null) {
        return null;
      }
      PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
      switch (pti.getPrimitiveCategory()) {
        case STRING:
          return t;
        case BYTE:
          return Byte.valueOf(t);
        case SHORT:
          Short s;
          return Short.valueOf(t);
        case INT:
          return Integer.valueOf(t);
        case LONG:
          return Long.valueOf(t);
        case FLOAT:
          return Float.valueOf(t);
        case DOUBLE:
          return Double.valueOf(t);
        case BOOLEAN:
          return Boolean.valueOf(t);
        case TIMESTAMP:
          return Timestamp.valueOf(t);
        case DATE:
          return Date.valueOf(t);
        case DECIMAL:
          return HiveDecimal.create(t);
        case CHAR:
          return new HiveChar(t, ((CharTypeInfo) typeInfo).getLength());
        case VARCHAR:
          return new HiveVarchar(t, ((VarcharTypeInfo)typeInfo).getLength());
        default:
          throw new IllegalArgumentException("Unsupported type " + typeInfo);
      }
    }
  }
}
