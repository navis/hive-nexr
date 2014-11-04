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

package org.apache.hadoop.hive.ql.io;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.hadoop.hive.serde2.MorphlineSerDe;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MorphlineValidator extends HiveRecordReader implements InputWrapper, JobConfigurable {

  Command morphline;
  String inputField;

  int maximum;
  int counter;

  public void setRecordReader(RecordReader recordReader) {
    if (!(recordReader.createValue() instanceof Text)) {
      throw new IllegalArgumentException("Morphline validator cannot applied to non-text values");
    }
    super.setRecordReader(recordReader);
  }

  @Override
  public boolean next(Object key, Object value) throws IOException {
    boolean hasNext;
    while ((hasNext = recordReader.next(key, value)) && process((Text)value)) {
    }
    if (!hasNext) {
      return false;
    }
    counter++;
    return maximum <= 0 || counter < maximum;
  }

  private final Record record = new Record();

  private boolean process(Text value) {
    record.getFields().clear();
    String inputValue = value.toString();
    record.put(inputField, inputValue);
    return morphline.process(record);
  }

  @Override
  public void configure(JobConf job) {
    String morphlineDef = job.get(MorphlineSerDe.MORPHLINE_DEFINITION);
    if (morphlineDef == null) {
      throw new RuntimeException(
          "This table does not have required serde property \"" + MorphlineSerDe.MORPHLINE_DEFINITION + "\"!");
    }

    String morphlineFile = morphlineDef;
    String morphlineID = null;
    int index = morphlineDef.indexOf('#');
    if (index >= 0) {
      morphlineFile = morphlineDef.substring(0, index);
      morphlineID = morphlineDef.substring(index + 1);
    }

    Compiler compiler = new Compiler();
    Config override = ConfigValueFactory.fromMap(getMorphlineProps(job)).toConfig();

    Config config;
    try {
      config = compiler.parse(new File(morphlineFile), override);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse morphline definition " + morphlineDef, e);
    }
    MorphlineContext context = new MorphlineContext.Builder().build();

    Config morph = compiler.find(morphlineID, config, morphlineFile);
    morphline = compiler.compile(morph, context, null);
    inputField = config.getString("inputField");

    maximum = job.getInt("input.validation.max", -1);
  }

  private Map<String, String> getMorphlineProps(JobConf jobConf) {
    Map<String, String> props = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : jobConf) {
      String key = entry.getKey();
      if (key.startsWith(MorphlineSerDe.MORPHLINE_PREFIX) &&
          !key.equals(MorphlineSerDe.MORPHLINE_DEFINITION)) {
        props.put(key.substring(MorphlineSerDe.MORPHLINE_PREFIX.length()), entry.getValue());
      }
    }
    return props;
  }
}
