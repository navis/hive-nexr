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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.RandomAccessibleHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LookupJoinOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(LookupJoinOptimizer.class.getName());

  public LookupJoinOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<JoinOperator, QBJoinTree> contexts = pctx.getJoinContext();
    if (contexts == null || contexts.isEmpty()) {
      return pctx;
    }
    Map<JoinOperator, QBJoinTree> newContext = new HashMap<JoinOperator, QBJoinTree>();
    for (Map.Entry<JoinOperator, QBJoinTree> entry : contexts.entrySet()) {
      JoinOperator joinOp = entry.getKey();
      for (TableScanOperator tableScanOp : find(joinOp, new ArrayList<TableScanOperator>(), TableScanOperator.class)) {
        Table tbl = pctx.getTopToTable().get(tableScanOp);
        if (tbl != null && tbl.getStorageHandler() instanceof RandomAccessibleHandler) {

        }
      }
    }
    return pctx;
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> find(Operator<?> operator, List<T> found, Class<T> clazz) {
    if (operator.getParentOperators() != null) {
      for (Operator<?> parent : operator.getParentOperators()) {
        if (clazz.isInstance(parent)) {
          found.add((T) parent);
        } else {
          find(parent, found, clazz);
        }
      }
    }
    return found;
  }
}
