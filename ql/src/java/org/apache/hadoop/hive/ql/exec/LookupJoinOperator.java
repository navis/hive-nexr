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

package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.LookupJoinDesc;
import org.apache.hadoop.hive.serde2.SerDeException;

public class LookupJoinOperator extends MachingJoinOperator<LookupJoinDesc> {

  Map<Byte, FetchTask> fetchTasks;

  public LookupJoinOperator() {
    super();
  }

  @Override
  protected void handleFirstRow() throws HiveException, SerDeException {
  }

  private transient List<Object> sink = new ArrayList<Object>();

  @Override
  protected MapJoinObjectValue getValueFor(AbstractMapJoinKey key, Byte pos) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public LookupJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

    /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  public static String getOperatorName() {
    return "LOOKUPJOIN";
  }
}
