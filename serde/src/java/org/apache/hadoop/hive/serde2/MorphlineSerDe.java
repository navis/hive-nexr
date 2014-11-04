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

package org.apache.hadoop.hive.serde2;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.util.MorphlineProcessor;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    MorphlineSerDe.MORPHLINE_ALL})
public class MorphlineSerDe extends AbstractSerDe {

  public static final Log LOG = LogFactory.getLog(MorphlineSerDe.class.getName());

  public static final String MORPHLINE_PREFIX = "morphline.";

  public static final String MORPHLINE_ALL = MORPHLINE_PREFIX + "*";
  public static final String MORPHLINE_DEFINITION = MORPHLINE_PREFIX + "definition"; // required

  private MorphlineProcessor processor;

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // We can get the table definition from tbl.
    // Read the configuration parameters
    String morphlineDef = tbl.getProperty(MORPHLINE_DEFINITION);
    if (morphlineDef == null) {
      throw new SerDeException(
          "This table does not have required serde property \"" + MORPHLINE_DEFINITION + "\"!");
    }

    String namesProps = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String typesProps = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    String[] columnNames = null;
    TypeInfo[] columnTypes = null;
    if (!StringUtils.isEmpty(namesProps)) {
      columnNames = namesProps.split(",");
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(typesProps).
          toArray(new TypeInfo[columnNames.length]);
    }

    Config properties = getMorphlineProps(tbl);
    try {
      processor = MorphlineProcessor.build(
          morphlineDef, columnNames, columnTypes, properties);
    } catch (Exception e) {
      throw new SerDeException("Failed to parse morphline definition", e);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return processor.getOutputOI();
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return processor.push(String.valueOf(blob));
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    throw new UnsupportedOperationException(
        "Morphline SerDe doesn't support the serialize() method");
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new UnsupportedOperationException(
        "Morphline SerDe doesn't support the serialize() method");
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  private Config getMorphlineProps(Properties tbl) {
    Map<String, String> props = new HashMap<String, String>();
    for (String key : tbl.stringPropertyNames()) {
      if (key.startsWith(MORPHLINE_PREFIX) && !key.equals(MORPHLINE_DEFINITION)) {
        props.put(key.substring(MORPHLINE_PREFIX.length()), tbl.getProperty(key));
      }
    }
    return ConfigFactory.parseMap(props);
  }
}
