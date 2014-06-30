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

package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;

/**
 * Extracts file pruning expression (used by MapredWork)
 */
public class FilePruningPredicateHandler implements HiveStoragePredicateHandler {

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
    analyzer.allowColumnName(VirtualColumn.FILENAME.getName());
    analyzer.setAllowAllFunctions(true);

    List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residual = analyzer.analyzePredicate(predicate, searchConditions);

    // there is no assertion that pushedPredicate to be applied always, so residue all of them.
    DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
    decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(searchConditions);

    // keep all (todo: can be removed when it's referenced by one alias)
    decomposedPredicate.residualPredicate = predicate;
    return decomposedPredicate;
  }
}
