/*
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
package org.apache.hadoop.hive.ql.ppd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Context for Expression Walker for determining predicate pushdown candidates
 * It contains a ExprInfo object for each expression that is processed.
 */
public class ExprWalkerInfo implements NodeProcessorCtx {

  protected static final Log LOG = LogFactory.getLog(OpProcFactory.class
      .getName());;

  public static final String UNKNOWN = "$$$";

  private Operator<? extends OperatorDesc> op = null;
  private RowResolver toRR = null;

  /**
   * Values the expression sub-trees (predicates) that can be pushed down for
   * root expression tree. Since there can be more than one alias in an
   * expression tree, this is a map from the alias to predicates.
   */
  private final Map<String, List<ExprNodeDesc>> pushdownPreds;

  /**
   * Values the expression sub-trees (predicates) that can not be pushed down for
   * root expression tree. Since there can be more than one alias in an
   * expression tree, this is a map from the alias to predicates.
   */
  private final List<ExprNodeDesc> nonFinalPreds;

  /**
   * this map contains a expr infos. Each key is a node in the expression tree
   * and the information for each node is the value which is used while walking
   * the tree by its parent.
   */
  private final Map<ExprNodeDesc, String> exprInfoMap;

  private boolean isDeterministic = true;

  public ExprWalkerInfo() {
    pushdownPreds = new HashMap<String, List<ExprNodeDesc>>();
    nonFinalPreds = new ArrayList<ExprNodeDesc>();
    exprInfoMap = new HashMap<ExprNodeDesc, String>();
  }

  public ExprWalkerInfo(Operator<? extends OperatorDesc> op, RowResolver toRR) {
    this.op = op;
    this.toRR = toRR;
    pushdownPreds = new HashMap<String, List<ExprNodeDesc>>();
    nonFinalPreds = new ArrayList<ExprNodeDesc>();
    exprInfoMap = new HashMap<ExprNodeDesc, String>();
  }

  /**
   * @return the op of this expression.
   */
  public Operator<? extends OperatorDesc> getOp() {
    return op;
  }

  /**
   * @return the row resolver of the operator of this expression.
   */
  public RowResolver getToRR() {
    return toRR;
  }

  /**
   * Returns alias for the predicate
   *
   * @param expr
   * @return alias
   */
  public String getCandidate(ExprNodeDesc expr) {
    return exprInfoMap.get(expr);
  }

  /**
   * Marks the specified expr to the specified alias
   *
   * @param expr
   * @param alias
   */
  public void setCandidate(ExprNodeDesc expr, String alias) {
    System.err.println("---- [ExprWalkerInfo/setCandidate] " + expr.getExprString() + " = " + alias);
    exprInfoMap.put(expr, alias == null ? null : alias.toLowerCase());
  }

  /**
   * Adds the specified expr as the top-most pushdown expr (ie all its children
   * can be pushed).
   *
   * @param expr
   */
  public void addFinalCandidate(String alias, ExprNodeDesc expr) {
    if (pushdownPreds.get(alias) == null) {
      pushdownPreds.put(alias, new ArrayList<ExprNodeDesc>());
    }
    pushdownPreds.get(alias).add(expr);
  }

  /**
   * Adds the passed list of pushDowns for the alias.
   *
   * @param alias
   * @param pushDowns
   */
  public void addPushDowns(String alias, List<ExprNodeDesc> pushDowns) {
    if (pushdownPreds.get(alias) == null) {
      pushdownPreds.put(alias, new ArrayList<ExprNodeDesc>());
    }
    pushdownPreds.get(alias).addAll(pushDowns);
  }

  /**
   * Returns the list of pushdown expressions for each alias that appear in the
   * current operator's RowResolver. The exprs in each list can be combined
   * using conjunction (AND).
   *
   * @return the map of alias to a list of pushdown predicates
   */
  public Map<String, List<ExprNodeDesc>> getFinalCandidates() {
    return pushdownPreds;
  }

  /**
   * Adds the specified expr as a non-final candidate
   *
   * @param expr
   */
  public void addNonFinalCandidate(ExprNodeDesc expr) {
    System.err.println("---- [ExprWalkerInfo/addNonFinalCandidates] " + expr);
    nonFinalPreds.add(expr);
  }

  public void addNonFinalCandidates(List<ExprNodeDesc> exprs) {
    System.err.println("---- [ExprWalkerInfo/addNonFinalCandidates] " + exprs);
    nonFinalPreds.addAll(exprs);
  }

  /**
   * Returns list of non-final candidate predicate for each map.
   *
   * @return list of non-final candidate predicates
   */
  public List<ExprNodeDesc> getNonFinalCandidates() {
    return nonFinalPreds;
  }

  /**
   * Merges the specified pushdown predicates with the current class.
   *
   * @param ewi
   *          ExpressionWalkerInfo
   */
  public void merge(ExprWalkerInfo ewi) {
    if (ewi == null) {
      return;
    }
    for (Map.Entry<String, List<ExprNodeDesc>> e : ewi.getFinalCandidates().entrySet()) {
      List<ExprNodeDesc> predList = pushdownPreds.get(e.getKey());
      if (predList != null) {
        predList.addAll(e.getValue());
      } else {
        pushdownPreds.put(e.getKey(), e.getValue());
      }
    }
    nonFinalPreds.addAll(ewi.nonFinalPreds);
    exprInfoMap.putAll(ewi.exprInfoMap);
  }

  /**
   * sets the deterministic flag for this expression.
   *
   * @param b
   *          deterministic or not
   */
  public void setDeterministic(boolean b) {
    isDeterministic = b;
  }

  /**
   * @return whether this expression is deterministic or not.
   */
  public boolean isDeterministic() {
    return isDeterministic;
  }

  public void pushDownCandidates(Operator<?> current) throws SemanticException {
    if (current.getColumnExprMap() != null) {
      List<ExprNodeDesc> input = new ArrayList<ExprNodeDesc>();
      for (Map.Entry<String, List<ExprNodeDesc>> e : pushdownPreds.entrySet()) {
        e.setValue(ExprNodeDescUtils.backtrackWith(e.getValue(), current));
      }
    }
  }

  // move non-qualified pushdownable predicates to non-final
  public void retain(Set<String> qualified) {
    Set<String> toRemove = new HashSet<String>();
    for (String alias : pushdownPreds.keySet()) {
      if (!qualified.contains(alias)) {
        toRemove.add(alias);
      }
    }
    for (String alias : toRemove) {
      nonFinalPreds.addAll(pushdownPreds.remove(alias));
    }
  }
}
