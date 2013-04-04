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
package org.apache.hadoop.hive.ql.ppd;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Expression factory for predicate pushdown processing. Each processor
 * determines whether the expression is a possible candidate for predicate
 * pushdown optimization for the given operator
 */
public final class ExprWalkerProcFactory {

  private static final Log LOG = LogFactory
      .getLog(ExprWalkerProcFactory.class.getName());

  /**
   * ColumnExprProcessor.
   *
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    /**
     * Converts the reference from child row resolver to current row resolver.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      ExprNodeColumnDesc colref = (ExprNodeColumnDesc) nd;
      Operator<? extends OperatorDesc> op = ctx.getOp();

      String candidateAlias = null;
      if (op.getColumnExprMap() != null) {
        // replace the output expression with the input expression so that
        // parent op can understand this expression
        ExprNodeDesc exp = op.getColumnExprMap().get(colref.getColumn());
        if (exp == null || exp instanceof ExprNodeGenericFuncDesc) {
          candidateAlias = ExprWalkerInfo.UNKNOWN;
        } else if (exp instanceof ExprNodeColumnDesc) {
          candidateAlias = ((ExprNodeColumnDesc)exp).getTabAlias();
        } else if (exp instanceof ExprNodeFieldDesc) {
          candidateAlias = ctx.getCandidate(((ExprNodeFieldDesc)exp).getDesc());
        }
      }
      if (candidateAlias == null) {
        String[] nm = ctx.getToRR().reverseLookup(colref.getColumn());
        candidateAlias = nm != null ? nm[0] : ExprWalkerInfo.UNKNOWN;
      }
      ctx.setCandidate(colref, candidateAlias);
      return candidateAlias;
    }

  }

  /**
   * FieldExprProcessor.
   *
   */
  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      ExprNodeFieldDesc expr = (ExprNodeFieldDesc) nd;
      String candidate = ctx.getCandidate(expr.getChildren().get(0));
      ctx.setCandidate(expr, candidate);
      return candidate;
    }
  }

  /**
   * If all children are candidates and refer only to one table alias then this
   * expr is a candidate else it is not a candidate but its children could be
   * final candidates.
   */
  public static class GenericFuncExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      String alias = null;
      ExprNodeGenericFuncDesc expr = (ExprNodeGenericFuncDesc) nd;

      if (!FunctionRegistry.isDeterministic(expr.getGenericUDF())) {
        // this GenericUDF can't be pushed down
        ctx.setCandidate(expr, ExprWalkerInfo.UNKNOWN);
        ctx.setDeterministic(false);
        return ExprWalkerInfo.UNKNOWN;
      }

      String candidateAlias = null;
      for (ExprNodeDesc child : expr.getChildren()) {
        String childAlias = ctx.getCandidate(child);
        if (childAlias == null) {
          continue;
        }
        if (childAlias.equals(ExprWalkerInfo.UNKNOWN) ||
            (candidateAlias != null && !candidateAlias.equals(childAlias))) {
          candidateAlias = ExprWalkerInfo.UNKNOWN;
          break;
        }
        candidateAlias = childAlias;
      }
      ctx.setCandidate(expr, candidateAlias);
      return candidateAlias;
    }
  }

  /**
   * For constants and null expressions.
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      ctx.setCandidate((ExprNodeDesc) nd, null);
      return true;
    }
  }

  public static NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  public static NodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  public static NodeProcessor getColumnProcessor() {
    return new ColumnExprProcessor();
  }

  private static NodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

  public static ExprWalkerInfo extractPushdownPreds(OpWalkerInfo opContext,
    Operator<? extends OperatorDesc> op, ExprNodeDesc pred)
    throws SemanticException {
    List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
    preds.add(pred);
    return extractPushdownPreds(opContext, op, preds);
  }

  /**
   * Extracts pushdown predicates from the given list of predicate expression.
   *
   * @param opContext
   *          operator context used for resolving column references
   * @param op
   *          operator of the predicates being processed
   * @param preds
   * @return The expression walker information
   * @throws SemanticException
   */
  public static ExprWalkerInfo extractPushdownPreds(OpWalkerInfo opContext,
    Operator<? extends OperatorDesc> op, List<ExprNodeDesc> preds)
    throws SemanticException {
    // Create the walker, the rules dispatcher and the context.
    ExprWalkerInfo exprContext = new ExprWalkerInfo(op, opContext
      .getRowResolver(op));

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(
        new RuleRegExp("R1", ExprNodeColumnDesc.class.getName() + "%"),
        getColumnProcessor());
    exprRules.put(
        new RuleRegExp("R2", ExprNodeFieldDesc.class.getName() + "%"),
        getFieldProcessor());
    exprRules.put(new RuleRegExp("R3", ExprNodeGenericFuncDesc.class.getName()
        + "%"), getGenericFuncProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        exprRules, exprContext);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>(preds);
    egw.startWalking(startNodes, null);

    HiveConf conf = opContext.getParseContext().getConf();
    // check the root expression for final candidates
    for (ExprNodeDesc pred : preds) {
      extractFinalCandidates(pred, exprContext, conf);
    }
    return exprContext;
  }

  /**
   * Walks through the top AND nodes and determine which of them are final
   * candidates.
   */
  private static void extractFinalCandidates(ExprNodeDesc expr,
      ExprWalkerInfo ctx, HiveConf conf) {
    if (FunctionRegistry.isOpAnd(expr)) {
      for (ExprNodeDesc child : expr.getChildren()) {
        extractFinalCandidates(child, ctx, conf);
      }
      return;
    }
    String candidate = ctx.getCandidate(expr);
    if (candidate == null || !candidate.equals(ExprWalkerInfo.UNKNOWN)) {
      ctx.addFinalCandidate(candidate, expr);
    } else {
      ctx.addNonFinalCandidate(expr);
    }
  }

  private ExprWalkerProcFactory() {
    // prevent instantiation
  }
}
