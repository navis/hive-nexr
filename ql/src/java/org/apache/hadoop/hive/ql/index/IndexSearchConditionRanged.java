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
package org.apache.hadoop.hive.ql.index;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Special IndexSearchCondition for handling between operator
 */
public class IndexSearchConditionRanged extends IndexSearchCondition {

  private ExprNodeConstantDesc minConstantDesc;
  private ExprNodeConstantDesc maxConstantDesc;

  public IndexSearchConditionRanged(ExprNodeColumnDesc column, String op, ExprNodeDesc expr) {
    super(column, op, null, expr);
  }

  @Override
  public void setConstantDesc(ExprNodeConstantDesc constantDesc) {
    throw new UnsupportedOperationException("setConstantDesc");
  }

  @Override
  public ExprNodeConstantDesc getConstantDesc() {
    throw new UnsupportedOperationException("getConstantDesc");
  }

  public ExprNodeConstantDesc getMinConstantDesc() {
    return minConstantDesc;
  }

  public void setMinConstantDesc(ExprNodeConstantDesc minConstantDesc) {
    this.minConstantDesc = minConstantDesc;
  }

  public ExprNodeConstantDesc getMaxConstantDesc() {
    return maxConstantDesc;
  }

  public void setMaxConstantDesc(ExprNodeConstantDesc maxConstantDesc) {
    this.maxConstantDesc = maxConstantDesc;
  }
}
