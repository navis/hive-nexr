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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;

/**
 * Operator factory for predicate pushdown processing of operator graph Each
 * operator determines the pushdown predicates by walking the expression tree.
 * Each operator merges its own pushdown predicates with those of its children
 * Finally the TableScan operator gathers all the predicates and inserts a
 * filter operator after itself. TODO: Further optimizations 1) Multi-insert
 * case 2) Create a filter operator for those predicates that couldn't be pushed
 * to the previous operators in the data flow 3) Merge multiple sequential
 * filter predicates into so that plans are more readable 4) Remove predicates
 * from filter operators that have been pushed. Currently these pushed
 * predicates are evaluated twice.
 */
public final class OpProcFactory {

  protected static final Log LOG = LogFactory.getLog(OpProcFactory.class
    .getName());

  /**
   * Processor for Script Operator Prevents any predicates being pushed.
   */
  public static class ScriptPPD extends DefaultPPD implements NodeProcessor {
    @Override
    protected Object process(Operator<?> operator, OpWalkerInfo owi) throws SemanticException {
      // script operator is a black-box to hive so no optimization here
      // assuming that nothing can be pushed above the script op
      // same with LIMIT op
      // create a filter with all children predicates
      createFinalFilter(operator, owi);
      return null;
    }
  }

  public static class LateralViewForwardPPD extends DefaultPPD implements NodeProcessor {
    @Override
    protected Object process(Operator<?> operator, OpWalkerInfo owi) throws SemanticException {
      // The lateral view forward operator has 2 children, a SELECT(*) and
      // a SELECT(cols) (for the UDTF operator) The child at index 0 is the
      // SELECT(*) because that's the way that the DAG was constructed. We
      // only want to get the predicates from the SELECT(*).
      Operator<?> selectOp = operator.getChildOperators().get(LateralViewJoinOperator.SELECT_TAG);
      ExprWalkerInfo childPreds = owi.getPrunedPreds(selectOp);
      owi.putPrunedPreds(operator, childPreds);
      return null;
    }
  }

  /**
   * Combines predicates of its child into a single expression and adds a filter
   * op as new child.
   */
  public static class TableScanPPD extends DefaultPPD implements NodeProcessor {
    @Override
    protected void pushDownCandidates(Operator<?> operator, OpWalkerInfo owi, ExprWalkerInfo ewi)
        throws SemanticException {
      // cannot pushdown further. create filter with candidates.
      Map<String, List<ExprNodeDesc>> candidates = ewi.getFinalCandidates();
      if (!candidates.isEmpty()) {
        OpProcFactory.createFilter(operator, candidates, owi);
      }
    }
  }

  /**
   * Determines the push down predicates in its where expression and then
   * combines it with the push down predicates that are passed from its children.
   */
  public static class FilterPPD extends DefaultPPD implements NodeProcessor {

    @Override
    protected Object process(Operator<?> operator, OpWalkerInfo owi) throws SemanticException {
      FilterOperator filterOp = (FilterOperator) operator;
      ExprNodeDesc predicate = filterOp.getConf().getPredicate();
      // Don't push a sampling predicate since createFilter() always creates filter
      // with isSamplePred = false. Also, the filterop with sampling pred is always
      // a child of TableScan, so there is no need to push this predicate.
      if (!filterOp.getConf().getIsSamplingPred()) {
        // get pushdown predicates for this operator's predicate
        ExprWalkerInfo ewi = ExprWalkerProcFactory.extractPushdownPreds(owi, filterOp, predicate);
        if (!ewi.isDeterministic()) {
          /* predicate is not deterministic */
          createFinalFilter(filterOp, owi);
          return null;
        }
        // add this filter for deletion, if it does not have non-final candidates
        if (ewi.getNonFinalCandidates().isEmpty()) {
          owi.addCandidateFilterOp(filterOp);
        }
        logExpr(filterOp, ewi);
        owi.putPrunedPreds(filterOp, ewi);
      }
      return super.process(operator, owi);
    }
  }

  public static class JoinPPD extends DefaultPPD {

    @Override
    protected final void createNonFinalFilter(Operator<?> operator, OpWalkerInfo owi, Set<String> includes) {
      Set<String> qualified = getQualifiedAliases((JoinOperator) operator);
      if (includes == null || !qualified.containsAll(includes)) {
        owi.getPrunedPreds(operator).retain(qualified);
      }
      super.createNonFinalFilter(operator, owi, includes);
    }

    @Override
    protected Object process(Operator<?> operator, OpWalkerInfo owi) throws SemanticException {
      Object result = super.process(operator, owi);
      HiveConf conf = owi.getParseContext().getConf();
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEPPDRECOGNIZETRANSITIVITY)) {
        applyFilterTransitivity((JoinOperator) operator, owi);
      }
      return result;
    }

    /**
     * Adds additional pushdown predicates for a join operator by replicating
     * filters transitively over all the equijoin conditions.
     *
     * If we have a predicate "t.col=1" and the equijoin conditions
     * "t.col=s.col" and "t.col=u.col", we add the filters "s.col=1" and
     * "u.col=1". Note that this does not depend on the types of joins (ie.
     * inner, left/right/full outer) between the tables s, t and u because if
     * a predicate, eg. "t.col=1" is present in getFinalCandidates() at this
     * point, we have already verified that it can be pushed down, so any rows
     * emitted must satisfy s.col=t.col=u.col=1 and replicating the filters
     * like this is ok.
     */
    private void applyFilterTransitivity(JoinOperator nd, OpWalkerInfo owi)
        throws SemanticException {
      ExprWalkerInfo prunePreds = owi.getPrunedPreds(nd);
      if (prunePreds != null) {
        // We want to use the row resolvers of the parents of the join op
        // because the rowresolver refers to the output columns of an operator
        // and the filters at this point refer to the input columns of the join
        // operator.
        Map<String, RowResolver> aliasToRR =
            new HashMap<String, RowResolver>();
        for (Operator<? extends OperatorDesc> o : (nd).getParentOperators()) {
          for (String alias : owi.getRowResolver(o).getTableNames()){
            aliasToRR.put(alias, owi.getRowResolver(o));
          }
        }

        // eqExpressions is a list of ArrayList<ASTNode>'s, one for each table
        // in the join. Then for each i, j and k, the join condition is that
        // eqExpressions[i][k]=eqExpressions[j][k] (*) (ie. the columns referenced
        // by the corresponding ASTNodes are equal). For example, if the query
        // was SELECT * FROM a join b on a.col=b.col and a.col2=b.col2 left
        // outer join c on b.col=c.col and b.col2=c.col2 WHERE c.col=1,
        // eqExpressions would be [[a.col1, a.col2], [b.col1, b.col2],
        // [c.col1, c.col2]].
        //
        // numEqualities is the number of equal columns in each equality
        // "chain" and numColumns is the number of such chains.
        //
        // Note that (*) is guaranteed to be true for the
        // join operator: if the equijoin condititions can't be expressed in
        // these equal-length lists of equal columns (for example if we had the
        // query SELECT * FROM a join b on a.col=b.col and a.col2=b.col2 left
        // outer join c on b.col=c.col), more than one join operator is used.
        ArrayList<ArrayList<ASTNode>> eqExpressions =
            owi.getParseContext().getJoinContext().get(nd).getExpressions();
        int numColumns = eqExpressions.size();
        int numEqualities = eqExpressions.get(0).size();

        // joins[i] is the join between table i and i+1 in the JoinOperator
        JoinCondDesc[] joins = (nd).getConf().getConds();

        // oldFilters contains the filters to be pushed down
        Map<String, List<ExprNodeDesc>> oldFilters =
            prunePreds.getFinalCandidates();
        Map<String, List<ExprNodeDesc>> newFilters =
            new HashMap<String, List<ExprNodeDesc>>();

        // We loop through for each chain of equalities
        for (int i=0; i<numEqualities; i++) {
          // equalColumns[i] is the ColumnInfo corresponding to the ith term
          // of the equality or null if the term is not a simple column
          // reference
          ColumnInfo[] equalColumns=new ColumnInfo[numColumns];
          for (int j=0; j<numColumns; j++) {
            equalColumns[j] =
                getColumnInfoFromAST(eqExpressions.get(j).get(i), aliasToRR);
          }
          for (int j=0; j<numColumns; j++) {
            for (int k=0; k<numColumns; k++) {
              if (j != k && equalColumns[j]!= null
                  && equalColumns[k] != null) {
                // terms j and k in the equality chain are simple columns,
                // so we can replace instances of column j with column k
                // in the filter and ad the replicated filter.
                ColumnInfo left = equalColumns[j];
                ColumnInfo right = equalColumns[k];
                if (oldFilters.get(left.getTabAlias()) != null){
                  for (ExprNodeDesc expr :
                    oldFilters.get(left.getTabAlias())) {
                    // Only replicate the filter if there is exactly one column
                    // referenced
                    Set<String> colsreferenced =
                        new HashSet<String>(expr.getCols());
                    if (colsreferenced.size() == 1
                        && colsreferenced.contains(left.getInternalName())){
                      ExprNodeDesc newexpr = expr.clone();
                      // Replace the column reference in the filter
                      replaceColumnReference(newexpr, left.getInternalName(),
                          right.getInternalName());
                      if (newFilters.get(right.getTabAlias()) == null) {
                        newFilters.put(right.getTabAlias(),
                            new ArrayList<ExprNodeDesc>());
                      }
                      newFilters.get(right.getTabAlias()).add(newexpr);
                    }
                  }
                }
              }
            }
          }
        }

        for (Entry<String, List<ExprNodeDesc>> aliasToFilters
            : newFilters.entrySet()){
          prunePreds.addPushDowns(aliasToFilters.getKey(), aliasToFilters.getValue());
        }
      }
    }

    /**
     * Replaces the ColumnInfo for the column referred to by an ASTNode
     * representing "table.column" or null if the ASTNode is not in that form
     */
    private ColumnInfo getColumnInfoFromAST(ASTNode nd,
        Map<String, RowResolver> aliastoRR) throws SemanticException {
      // this bit is messy since we are parsing an ASTNode at this point
      if (nd.getType()==HiveParser.DOT) {
        if (nd.getChildCount()==2) {
          if (nd.getChild(0).getType()==HiveParser.TOK_TABLE_OR_COL
              && nd.getChild(0).getChildCount()==1
              && nd.getChild(1).getType()==HiveParser.Identifier){
            // We unescape the identifiers and make them lower case--this
            // really shouldn't be done here, but getExpressions gives us the
            // raw ASTNodes. The same thing is done in SemanticAnalyzer.
            // parseJoinCondPopulateAlias().
            String alias = BaseSemanticAnalyzer.unescapeIdentifier(
                nd.getChild(0).getChild(0).getText().toLowerCase());
            String column = BaseSemanticAnalyzer.unescapeIdentifier(
                nd.getChild(1).getText().toLowerCase());
            RowResolver rr=aliastoRR.get(alias);
            if (rr == null) {
              return null;
            }
            return rr.get(alias, column);
          }
        }
      }
      return null;
    }

    /**
     * Replaces all instances of oldColumn with newColumn in the
     * ExprColumnDesc's of the ExprNodeDesc
     */
    private void replaceColumnReference(ExprNodeDesc expr,
        String oldColumn, String newColumn) {
      if (expr instanceof ExprNodeColumnDesc) {
        if (((ExprNodeColumnDesc) expr).getColumn().equals(oldColumn)){
          ((ExprNodeColumnDesc) expr).setColumn(newColumn);
        }
      }

      if (expr.getChildren() != null){
        for (ExprNodeDesc childexpr : expr.getChildren()) {
          replaceColumnReference(childexpr, oldColumn, newColumn);
        }
      }
    }

    /**
     * Figures out the aliases for whom it is safe to push predicates based on
     * ANSI SQL semantics. The join conditions are left associative so "a
     * RIGHT OUTER JOIN b LEFT OUTER JOIN c INNER JOIN d" is interpreted as
     * "((a RIGHT OUTER JOIN b) LEFT OUTER JOIN c) INNER JOIN d".  For inner
     * joins, both the left and right join subexpressions are considered for
     * pushing down aliases, for the right outer join, the right subexpression
     * is considered and the left ignored and for the left outer join, the
     * left subexpression is considered and the left ignored. Here, aliases b
     * and d are eligible to be pushed up.
     *
     * TODO: further optimization opportunity for the case a.c1 = b.c1 and b.c2
     * = c.c2 a and b are first joined and then the result with c. But the
     * second join op currently treats a and b as separate aliases and thus
     * disallowing predicate expr containing both tables a and b (such as a.c3
     * + a.c4 > 20). Such predicates also can be pushed just above the second
     * join and below the first join
     *
     * @param op
     *          Join Operator
     *
     * @return set of qualified aliases
     */
    private Set<String> getQualifiedAliases(JoinOperator op) {
      Set<String> aliases = new HashSet<String>();
      JoinCondDesc[] conds = op.getConf().getConds();
      Map<Integer, Set<String>> posToAliasMap = op.getPosToAliasMap();
      int i;
      for (i=conds.length-1; i>=0; i--){
        if (conds[i].getType() == JoinDesc.INNER_JOIN) {
          aliases.addAll(posToAliasMap.get(i+1));
        } else if (conds[i].getType() == JoinDesc.FULL_OUTER_JOIN) {
          break;
        } else if (conds[i].getType() == JoinDesc.RIGHT_OUTER_JOIN) {
          aliases.addAll(posToAliasMap.get(i+1));
          break;
        } else if (conds[i].getType() == JoinDesc.LEFT_OUTER_JOIN) {
          continue;
        }
      }
      if(i == -1){
        aliases.addAll(posToAliasMap.get(0));
      }
      return aliases;
    }
  }

  /**
   * Processor for ReduceSink operator.
   *
   */
  public static class ReduceSinkPPD extends DefaultPPD implements NodeProcessor {

    @Override
    protected Set<String> getQualifiedAliases(Operator<?> operator, OpWalkerInfo owi) {
      assert operator.getNumChild() == 1;
      Operator<?> child = operator.getChildOperators().get(0);
      if (child instanceof JoinOperator) {
        int pos = child.getParentOperators().indexOf(operator);
        Set<String> aliases = ((JoinOperator)child).getPosToAliasMap().get(pos);
        if (aliases == null) {
          // this side does not contributes to output (semi-join, for example)
          return Collections.emptySet();
        }
        return aliases;
      }
      return super.getQualifiedAliases(operator, owi);
    }
  }

  /**
   * Default processor which just merges its children.
   */
  public static class DefaultPPD implements NodeProcessor {

    @Override
    public final Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing for " + nd.toString());
      System.err.println("[OpProcFactory$DefaultPPD/process] " + nd);
      return process((Operator<?>) nd, (OpWalkerInfo) procCtx);
    }

    protected Object process(Operator<?> operator, OpWalkerInfo owi) throws SemanticException {
      Set<String> includes = getQualifiedAliases(operator, owi);
      System.err.println("- [OpProcFactory$DefaultPPD/process] " + includes + ":" + owi.getRowResolver(operator).getTableNames());
      if (mergeWithChildrenPred(operator, owi, includes)) {
        ExprWalkerInfo ewi = owi.getPrunedPreds(operator);
        System.err.println("--I [OpProcFactory$DefaultPPD/process] " + ewi.getFinalCandidates());
        System.err.println("--I [OpProcFactory$DefaultPPD/process] " + ewi.getNonFinalCandidates());
        createNonFinalFilter(operator, owi, includes);
      }
      ExprWalkerInfo ewi = owi.getPrunedPreds(operator);
      if (ewi != null && !ewi.getFinalCandidates().isEmpty()) {
        pushDownCandidates(operator, owi, ewi);
        System.err.println("--O [OpProcFactory$DefaultPPD/process] " + ewi.getFinalCandidates());
      }
      return null;
    }

    protected void pushDownCandidates(Operator<?> operator, OpWalkerInfo owi, ExprWalkerInfo ewi)
        throws SemanticException {
      ewi.pushDownCandidates(operator);
    }

    protected Set<String> getQualifiedAliases(Operator<?> operator, OpWalkerInfo owi) {
      List<Operator<? extends OperatorDesc>> children = operator.getChildOperators();
      if (children != null && !children.isEmpty()) {
        for (Operator<?> child : operator.getChildOperators()) {
          if (child instanceof UnionOperator) {
            continue;       // forwards filters to both sides
          }
          if (child.getNumParent() > 1) {
            Set<String> aliases = owi.getRowResolver(operator).getTableNames();
            if (aliases.size() == 1 && aliases.contains("")) {
              return null;  // Reduce sink of group by operator
            }
            return aliases;
          }
        }
      }
      return null;    // forward all filters if possible
    }

    protected void createFinalFilter(Operator<?> operator, OpWalkerInfo owi)
        throws SemanticException {
      Map<String, List<ExprNodeDesc>> predicates = mergeChildrenPred(operator, owi);
      if (!predicates.isEmpty()) {
        OpProcFactory.createFilter(operator, predicates, owi);
      }
    }

    protected void createNonFinalFilter(Operator<?> operator, OpWalkerInfo owi, Set<String> includes) {
      ExprWalkerInfo pruned = owi.getPrunedPreds(operator);
      List<ExprNodeDesc> residual = pruned.getNonFinalCandidates();
      if (!residual.isEmpty()) {
        OpProcFactory.createFilter(operator, residual, owi);
      }
      residual.clear();
    }

    /**
     * @param nd
     * @param ewi
     */
    protected void logExpr(Node nd, ExprWalkerInfo ewi) {
      for (Entry<String, List<ExprNodeDesc>> e : ewi.getFinalCandidates()
          .entrySet()) {
        LOG.info("Pushdown Predicates of " + nd.getName() + " For Alias : "
            + e.getKey());
        for (ExprNodeDesc n : e.getValue()) {
          LOG.info("\t" + n.getExprString());
        }
      }
    }

    /**
     * Take current operators pushdown predicates and merges them with
     * children's pushdown predicates.
     *
     *
     * @param current
     *          current operator
     * @param owi
     *          operator context during this walk
     * @param includes
     *          includes that this operator can pushdown. null means that all
     *          includes can be pushed down
     * @throws SemanticException
     */
    protected boolean mergeWithChildrenPred(Operator<?> current, OpWalkerInfo owi, Set<String> includes)
        throws SemanticException {
      if (current.getNumChild() == 0) {
        return false;
      }
      if (current.getNumChild() > 1) {
        // ppd for multi-insert query is not yet implemented (extract common set?)
        // no-op for leafs
        for (Operator<?> child : current.getChildOperators()) {
          removeCandidates(child, owi); // remove candidated filters on this branch
        }
        return false;
      }
      Operator<? extends OperatorDesc> child = current.getChildOperators().get(0);
      ExprWalkerInfo childPreds = owi.getPrunedPreds(child);
      if (childPreds == null) {
        return false;
      }
      ExprWalkerInfo ewi = owi.getPrunedPreds(current);
      if (ewi == null) {
        owi.putPrunedPreds(current, ewi = new ExprWalkerInfo());
      }
      for (Entry<String, List<ExprNodeDesc>> e : childPreds
          .getFinalCandidates().entrySet()) {
        if (includes == null || includes.contains(e.getKey()) || e.getKey() == null) {
          // e.getKey() (alias) can be null in case of constant expressions. see
          // input8.q
          ExprWalkerInfo extractPushdownPreds = ExprWalkerProcFactory
            .extractPushdownPreds(owi, current, e.getValue());
          ewi.merge(extractPushdownPreds);
          logExpr(current, extractPushdownPreds);
        }
      }
      owi.putPrunedPreds(current, ewi);
      return true;
    }

    private void removeCandidates(Operator<?> operator, OpWalkerInfo owi) {
      if (operator instanceof FilterOperator) {
        owi.removeCandidateFilterOp((FilterOperator) operator);
      }
      if (operator.getChildOperators() != null) {
        for (Operator<?> child : operator.getChildOperators()) {
          removeCandidates(child, owi);
        }
      }
    }

    protected Map<String, List<ExprNodeDesc>> mergeChildrenPred(Operator<?> operator, OpWalkerInfo owi)
        throws SemanticException {
      if (operator.getNumChild() == 0) {
        return Collections.emptyMap();
      }
      ExprWalkerInfo ewi = new ExprWalkerInfo();
      for (Operator<? extends OperatorDesc> child : operator.getChildOperators()) {
        ExprWalkerInfo childPreds = owi.getPrunedPreds(child);
        if (childPreds == null) {
          continue;
        }
        for (Entry<String, List<ExprNodeDesc>> e : childPreds
            .getFinalCandidates().entrySet()) {
          ewi.addPushDowns(e.getKey(), e.getValue());
          logExpr(operator, ewi);
        }
      }
      return ewi.getFinalCandidates();
    }
  }

  protected static Object createFilter(Operator op,
      Map<String, List<ExprNodeDesc>> predicates, OpWalkerInfo owi) {
    // combine all predicates into a single expression
    List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
    for (List<ExprNodeDesc> predicate : predicates.values()) {
      preds.addAll(predicate);
    }
    return createFilter(op, preds, owi);
  }

  protected static Object createFilter(Operator op,
      List<ExprNodeDesc> predicates, OpWalkerInfo owi) {
    RowResolver inputRR = owi.getRowResolver(op);

    // combine all predicates into a single expression
    List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
    for (ExprNodeDesc pred : predicates) {
      preds = ExprNodeDescUtils.split(pred, preds);
    }
    System.err.println("---- [OpProcFactory/createFilter] " + preds);
    if (preds.isEmpty()) {
      return null;
    }

    ExprNodeDesc condn = ExprNodeDescUtils.mergePredicates(preds);
    if (!(condn instanceof ExprNodeGenericFuncDesc)) {
      condn = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
          FunctionRegistry.getGenericUDFForName("boolean"), Arrays.asList(condn));
    }

    if (op instanceof TableScanOperator) {
      boolean pushFilterToStorage;
      HiveConf hiveConf = owi.getParseContext().getConf();
      pushFilterToStorage =
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVEOPTPPD_STORAGE);
      if (pushFilterToStorage) {
        condn = pushFilterToStorageHandler(
          (TableScanOperator) op,
          (ExprNodeGenericFuncDesc)condn,
          owi,
          hiveConf);
        if (condn == null) {
          // we pushed the whole thing down
          return null;
        }
      }
    }

    // add new filter op
    List<Operator<? extends OperatorDesc>> originalChilren = op
        .getChildOperators();
    op.setChildOperators(null);
    Operator<FilterDesc> output = OperatorFactory.getAndMakeChild(
        new FilterDesc(condn, false), new RowSchema(inputRR.getColumnInfos()),
        op);
    output.setChildOperators(originalChilren);
    for (Operator<? extends OperatorDesc> ch : originalChilren) {
      List<Operator<? extends OperatorDesc>> parentOperators = ch
          .getParentOperators();
      int pos = parentOperators.indexOf(op);
      assert pos != -1;
      parentOperators.remove(pos);
      parentOperators.add(pos, output); // add the new op as the old
    }
    OpParseContext ctx = new OpParseContext(inputRR);
    owi.put(output, ctx);

    return output;
  }

  public static void removeOriginalFilters(OpWalkerInfo owi) {
    // remove the candidate filter ops
    for (FilterOperator fop : owi.getCandidateFilterOps()) {
      List<Operator<? extends OperatorDesc>> children = fop.getChildOperators();
      List<Operator<? extends OperatorDesc>> parents = fop.getParentOperators();
      for (Operator<? extends OperatorDesc> parent : parents) {
        parent.getChildOperators().addAll(children);
        parent.removeChild(fop);
      }
      for (Operator<? extends OperatorDesc> child : children) {
        child.getParentOperators().addAll(parents);
        child.removeParent(fop);
      }
    }
    owi.getCandidateFilterOps().clear();
  }

  /**
   * Attempts to push a predicate down into a storage handler.  For
   * native tables, this is a no-op.
   *
   * @param tableScanOp table scan against which predicate applies
   *
   * @param originalPredicate predicate to be pushed down
   *
   * @param owi object walk info
   *
   * @param hiveConf Hive configuration
   *
   * @return portion of predicate which needs to be evaluated
   * by Hive as a post-filter, or null if it was possible
   * to push down the entire predicate
   */
  private static ExprNodeGenericFuncDesc pushFilterToStorageHandler(
    TableScanOperator tableScanOp,
    ExprNodeGenericFuncDesc originalPredicate,
    OpWalkerInfo owi,
    HiveConf hiveConf) {

    TableScanDesc tableScanDesc = tableScanOp.getConf();
    Table tbl = owi.getParseContext().getTopToTable().get(tableScanOp);
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTINDEXFILTER)) {
      // attach the original predicate to the table scan operator for index
      // optimizations that require the pushed predicate before pcr & later
      // optimizations are applied
      tableScanDesc.setFilterExpr(originalPredicate);
    }
    if (!tbl.isNonNative()) {
      return originalPredicate;
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();
    if (!(storageHandler instanceof HiveStoragePredicateHandler)) {
      // The storage handler does not provide predicate decomposition
      // support, so we'll implement the entire filter in Hive.  However,
      // we still provide the full predicate to the storage handler in
      // case it wants to do any of its own prefiltering.
      tableScanDesc.setFilterExpr(originalPredicate);
      return originalPredicate;
    }
    HiveStoragePredicateHandler predicateHandler =
      (HiveStoragePredicateHandler) storageHandler;
    JobConf jobConf = new JobConf(owi.getParseContext().getConf());
    Utilities.setColumnNameList(jobConf, tableScanOp);
    Utilities.setColumnTypeList(jobConf, tableScanOp);
    Utilities.copyTableJobPropertiesToConf(
      Utilities.getTableDesc(tbl),
      jobConf);
    Deserializer deserializer = tbl.getDeserializer();
    HiveStoragePredicateHandler.DecomposedPredicate decomposed =
      predicateHandler.decomposePredicate(
        jobConf,
        deserializer,
        originalPredicate);
    if (decomposed == null) {
      // not able to push anything down
      if (LOG.isDebugEnabled()) {
        LOG.debug("No pushdown possible for predicate:  "
          + originalPredicate.getExprString());
      }
      return originalPredicate;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Original predicate:  "
        + originalPredicate.getExprString());
      if (decomposed.pushedPredicate != null) {
        LOG.debug(
          "Pushed predicate:  "
          + decomposed.pushedPredicate.getExprString());
      }
      if (decomposed.residualPredicate != null) {
        LOG.debug(
          "Residual predicate:  "
          + decomposed.residualPredicate.getExprString());
      }
    }
    tableScanDesc.setFilterExpr(decomposed.pushedPredicate);
    return decomposed.residualPredicate;
  }

  public static NodeProcessor getFilterProc() {
    return new FilterPPD();
  }

  public static NodeProcessor getJoinProc() {
    return new JoinPPD();
  }

  public static NodeProcessor getRSProc() {
    return new ReduceSinkPPD();
  }

  public static NodeProcessor getTSProc() {
    return new TableScanPPD();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultPPD();
  }

  public static NodeProcessor getPTFProc() {
    return new ScriptPPD();
  }

  public static NodeProcessor getSCRProc() {
    return new ScriptPPD();
  }

  public static NodeProcessor getLIMProc() {
    return new ScriptPPD();
  }

  public static NodeProcessor getLVFProc() {
    return new LateralViewForwardPPD();
  }

  private OpProcFactory() {
    // prevent instantiation
  }
}
