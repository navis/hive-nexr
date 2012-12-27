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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

public class SamplingContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private int samplingNum = 200;
  private List<ExprNodeDesc> samplingKeys;
  private TableDesc tableInfo;

  @Explain(displayName = "Sampling Number")
  public int getSamplingNum() {
    return samplingNum;
  }

  public void setSamplingNum(int samplingNum) {
    this.samplingNum = samplingNum;
  }

  @Explain(displayName = "Sampling Keys")
  public List<ExprNodeDesc> getSamplingKeys() {
    return samplingKeys;
  }

  public void setSamplingKeys(List<ExprNodeDesc> samplingKeys) {
    this.samplingKeys = samplingKeys;
  }

  public TableDesc getTableInfo() {
    return tableInfo;
  }

  public void setTableInfo(TableDesc tableInfo) {
    this.tableInfo = tableInfo;
  }
}
