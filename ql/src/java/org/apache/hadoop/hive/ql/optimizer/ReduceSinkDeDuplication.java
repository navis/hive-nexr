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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEGROUPBYSKEW;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESCRIPTOPERATORTRUST;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESKEWJOIN;

/**
 * If two reducer sink operators share the same partition/sort columns, we
 * should merge them. This should happen after map join optimization because map
 * join optimization will remove reduce sink operators.
 */
public class ReduceSinkDeDuplication implements Transform{

  protected ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

 // generate pruned column list for all relevant operators
    ReduceSinkDeduplicateProcCtx cppCtx = new ReduceSinkDeduplicateProcCtx(pGraphContext);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "RS%.*RS%"), ReduceSinkDeduplicateProcFactory
        .getReducerReducerProc());
    opRules.put(new RuleRegExp("R2", "RS%.*RS%GBY%"), ReduceSinkDeduplicateProcFactory
        .getReducerGroupbyProc());
    opRules.put(new RuleRegExp("R3", "RS%GBY%.*RS%"), ReduceSinkDeduplicateProcFactory
        .getGroupbyReducerProc());
    opRules.put(new RuleRegExp("R3", "RS%GBY%.*RS%GBY%"), ReduceSinkDeduplicateProcFactory
        .getGroupbyGroupbyProc());
    opRules.put(new RuleRegExp("R4", "JOIN%.*RS%"), ReduceSinkDeduplicateProcFactory
        .getJoinReducerProc());
    opRules.put(new RuleRegExp("R5", "JOIN%.*RS%GBY%"), ReduceSinkDeduplicateProcFactory
        .getJoinGbyReducerProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(ReduceSinkDeduplicateProcFactory
        .getDefaultProc(), opRules, cppCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  class ReduceSinkDeduplicateProcCtx implements NodeProcessorCtx{
    ParseContext pctx;
    List<Operator<?>> removedOps;

    public ReduceSinkDeduplicateProcCtx(ParseContext pctx) {
      removedOps = new ArrayList<Operator<?>>();
      this.pctx = pctx;
    }

    public boolean contains (Operator<?> rsOp) {
      return removedOps.contains(rsOp);
    }

    public void addRemovedOperator(Operator<?> rsOp) {
      if (!removedOps.contains(rsOp)) {
        removedOps.add(rsOp);
      }
    }

    public ParseContext getPctx() {
      return pctx;
    }

    public void setPctx(ParseContext pctx) {
      this.pctx = pctx;
    }
  }

  static class ReduceSinkDeduplicateProcFactory {

    public static NodeProcessor getReducerReducerProc() {
      return new ReducerReducerProc();
    }

    public static NodeProcessor getReducerGroupbyProc() {
      return new ReducerGroupbyProc();
    }

    public static NodeProcessor getGroupbyReducerProc() {
      return new GroupbyReducerProc();
    }

    public static NodeProcessor getGroupbyGroupbyProc() {
      return new GroupbyGroupbyProc();
    }

    public static NodeProcessor getJoinReducerProc() {
      return new JoinReducerProc();
    }

    public static NodeProcessor getJoinGbyReducerProc() {
      return new JoinGroupbyProc();
    }

    public static NodeProcessor getDefaultProc() {
      return new DefaultProc();
    }

    /*
     * do nothing.
     */
    static class DefaultProc implements NodeProcessor {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        return null;
      }
    }

    abstract static class AbsctractReducerReducerProc implements NodeProcessor {

      ReduceSinkDeduplicateProcCtx dedupCtx;

      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {
        dedupCtx = (ReduceSinkDeduplicateProcCtx) procCtx;
        if (!dedupCtx.contains((Operator<?>) nd)) {
          return process(nd, stack, dedupCtx.getPctx());
        }
        return false;
      }

      protected abstract Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException;

      protected boolean merge(ReduceSinkOperator childRS, ReduceSinkOperator parentRS) {
        return merge(childRS, parentRS, 0);
      }

      protected boolean merge(ReduceSinkOperator childRS, ReduceSinkOperator parentRS, int tag) {
        int[] result = checkStatus(childRS, parentRS, tag);
        if (result == null) {
          return false;
        }
        if (result[0] > 0) {
          Operator<?> terminal = getSingleParent(parentRS, tag);
          ArrayList<ExprNodeDesc> childKCs = childRS.getConf().getKeyCols();
          parentRS.getConf().setKeyCols(backtrack(childKCs, childRS, terminal, tag));
        }
        if (result[1] > 0) {
          Operator<?> terminal = getSingleParent(parentRS, tag);
          ArrayList<ExprNodeDesc> childPCs = childRS.getConf().getPartitionCols();
          parentRS.getConf().setPartitionCols(backtrack(childPCs, childRS, terminal, tag));
        }
        if (result[2] > 0) {
          parentRS.getConf().setOrder(childRS.getConf().getOrder());
        }
        if (result[3] > 0) {
          parentRS.getConf().setNumReducers(childRS.getConf().getNumReducers());
        }
        return true;
      }

      // -1 for p to c, 1 for c to p
      private int[] checkStatus(ReduceSinkOperator childRS, ReduceSinkOperator parentRS, int tag) {
        List<ExprNodeDesc> ckeys = childRS.getConf().getKeyCols();
        List<ExprNodeDesc> pkeys = parentRS.getConf().getKeyCols();
        Integer moveKeyColTo = checkExprs(ckeys, pkeys, childRS, parentRS, tag);
        if (moveKeyColTo == null) {
          return null;
        }
        List<ExprNodeDesc> cpars = childRS.getConf().getPartitionCols();
        List<ExprNodeDesc> ppars = parentRS.getConf().getPartitionCols();
        Integer movePartitionColTo = checkExprs(cpars, ppars, childRS, parentRS, tag);
        if (movePartitionColTo == null) {
          return null;
        }
        int moveRSOrderTo = 0;
        String corder = childRS.getConf().getOrder();
        String porder = parentRS.getConf().getOrder();
        if (corder == null || corder.trim().equals("")) {
          if (porder != null && !porder.trim().equals("")) {
            moveRSOrderTo = -1;
          }
        } else {
          if (porder == null || porder.trim().equals("")) {
            moveRSOrderTo = 1;
          } else {
            corder = corder.trim();
            porder = porder.trim();
            int target = Math.min(corder.length(), porder.length());
            if (!corder.substring(0, target).equals(porder.substring(0, target))) {
              return null;
            }
            moveRSOrderTo = Integer.valueOf(corder.length()).compareTo(porder.length());
          }
        }
        int moveReducerNumTo = 0;
        int creduce = childRS.getConf().getNumReducers();
        int preduce = parentRS.getConf().getNumReducers();
        if (creduce < 0) {
          if (preduce >= 0) {
            moveReducerNumTo = -1;
          }
        } else {
          if (preduce < 0) {
            moveReducerNumTo = 1;
          } else {
            if (creduce != preduce) {
              return null;
            }
          }
        }
        return new int[] {moveKeyColTo, movePartitionColTo, moveRSOrderTo, moveReducerNumTo};
      }

      private Integer checkExprs(List<ExprNodeDesc> ckeys, List<ExprNodeDesc> pkeys,
          ReduceSinkOperator childRS, ReduceSinkOperator parentRS, int tag) {
        Integer moveKeyColTo = 0;
        if (ckeys == null || ckeys.isEmpty()) {
          if (pkeys != null && !pkeys.isEmpty()) {
            Operator<?> terminal = getSingleParent(parentRS);
            for (ExprNodeDesc pkey : pkeys) {
              if (backtrack(pkey, parentRS, terminal, tag) == null) {
                return null;
              }
            }
            moveKeyColTo = -1;
          }
        } else {
          if (pkeys == null || pkeys.isEmpty()) {
            Operator<?> terminal = getSingleParent(parentRS);
            for (ExprNodeDesc ckey : ckeys) {
              if (backtrack(ckey, childRS, terminal, tag) == null) {
                return null;
              }
            }
            moveKeyColTo = 1;
          } else {
            moveKeyColTo = sameKeys(ckeys, pkeys, childRS, parentRS, tag);
          }
        }
        return moveKeyColTo;
      }

      protected Integer sameKeys(List<ExprNodeDesc> cexprs, List<ExprNodeDesc> pexprs,
          Operator<?> child, Operator<?> parent, int tag) {
        int common = Math.min(cexprs.size(), pexprs.size());
        int limit = Math.max(cexprs.size(), pexprs.size());
        Operator<?> terminal = getSingleParent(parent);
        int i = 0;
        for (; i < common; i++) {
          ExprNodeDesc cexpr = backtrack(cexprs.get(i), child, terminal, tag);
          ExprNodeDesc pexpr = backtrack(pexprs.get(i), parent, terminal, tag);
          if (cexpr == null || !cexpr.isSame(pexpr)) {
            return null;
          }
        }
        for (;i < limit; i++) {
          if (cexprs.size() > pexprs.size()) {
            if (backtrack(cexprs.get(i), child, terminal, tag) == null) {
              return null;
            }
          } else if (backtrack(pexprs.get(i), parent, terminal, tag) == null) {
            return null;
          }
        }
        return Integer.valueOf(cexprs.size()).compareTo(pexprs.size());
      }

      private ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources,
          Operator<?> current, Operator<?> terminal, int tag) {
        ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>();
        for (ExprNodeDesc expr : sources) {
          result.add(backtrack(expr, current, terminal, tag));
        }
        return result;
      }

      private ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current,
          Operator<?> terminal, int tag) {
        if (source instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc function = (ExprNodeGenericFuncDesc) source.clone();
          List<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
          for (ExprNodeDesc param : function.getChildren()) {
            params.add(backtrack(param, current, terminal, tag));
          }
          function.setChildExprs(params);
          return function;
        }
        if (source instanceof ExprNodeColumnDesc) {
          ExprNodeColumnDesc column = (ExprNodeColumnDesc) source;
          return backtrack(column, current, terminal, tag);
        }
        if (source instanceof ExprNodeFieldDesc) {
          ExprNodeFieldDesc field = (ExprNodeFieldDesc) source;
          String name = field.getFieldName();
          TypeInfo type = field.getTypeInfo();
          ExprNodeDesc backtrack = backtrack(field.getDesc(), current, terminal, tag);
          return new ExprNodeFieldDesc(type, backtrack, name, field.getIsList());
        }
        return source;
      }

      private ExprNodeDesc backtrack(ExprNodeColumnDesc column, Operator<?> current,
          Operator<?> terminal, int tag) {
        if (current == null || current == terminal) {
          return column;
        }
        Map<String, ExprNodeDesc> mapping = current.getColumnExprMap();
        if (mapping == null || !mapping.containsKey(column.getColumn())) {
          return backtrack(column, getSingleParent(current, tag), terminal, tag);
        }
        ExprNodeDesc mapped = mapping.get(column.getColumn());
        if (mapped instanceof ExprNodeColumnDesc) {
          String table1 = column.getTabAlias();
          String table2 = ((ExprNodeColumnDesc)mapped).getTabAlias();
          if (table1 != null && !table1.isEmpty() && !table1.equals(table2)) {
            return null;
          }
        }
        return backtrack(mapped, getSingleParent(current, tag), terminal, tag);
      }

      protected Operator<?> getSingleParent(Operator<?> operator) {
        return getSingleParent(operator, false);
      }

      protected Operator<?> getSingleParent(Operator<?> operator, boolean skipForward) {
        if (operator.getParentOperators() != null && operator.getParentOperators().size() == 1) {
          Operator<?> parent = operator.getParentOperators().get(0);
          if (skipForward && parent instanceof ForwardOperator) {
            return getSingleParent(parent, skipForward);
          }
          return parent;
        }
        return null;
      }

      protected Operator<?> getSingleChild(Operator<?> operator) {
        return getSingleChild(operator, false);
      }

      protected Operator<?> getSingleChild(Operator<?> operator, boolean skipForward) {
        if (operator.getChildOperators() != null && operator.getChildOperators().size() == 1) {
          Operator<?> child = operator.getChildOperators().get(0);
          if (skipForward && child instanceof ForwardOperator) {
            return getSingleChild(child, skipForward);
          }
          return child;
        }
        return null;
      }

      protected Operator<?> getSingleParent(Operator<?> operator, int tag) {
        return operator instanceof JoinOperator ? operator.getParentOperators().get(tag)
            : getSingleParent(operator);
      }

      @SuppressWarnings("unchecked")
      protected <T extends Operator<?>> T[] findParentOperators(Operator<?> start,
          Class<T> target, boolean trustScript) {
        Operator<?> cursor = getSingleParent(start);
        for (; cursor != null; cursor = getSingleParent(cursor)) {
          if (target.isAssignableFrom(cursor.getClass())) {
            T[] array = (T[]) Array.newInstance(target, 1);
            array[0] = (T) cursor;
            return array;
          }
          if (cursor instanceof JoinOperator) {
            return findParentOperators((JoinOperator) cursor, target);
          }
          if (cursor instanceof ScriptOperator && !trustScript) {
            return null;
          }
          if (!(cursor instanceof SelectOperator
              || cursor instanceof FilterOperator
              || cursor instanceof ExtractOperator
              || cursor instanceof ForwardOperator
              || cursor instanceof ScriptOperator
              || cursor instanceof ReduceSinkOperator)) {
            return null;
          }
        }
        return null;
      }

      @SuppressWarnings("unchecked")
      private <T extends Operator<?>> T[] findParentOperators(JoinOperator join, Class<T> target) {
        List<Operator<? extends Serializable>> parents = join.getParentOperators();
        T[] result = (T[]) Array.newInstance(target, parents.size());
        for (int tag = 0; tag < result.length; tag++) {
          Operator<?> cursor = parents.get(tag);
          for (; cursor != null; cursor = getSingleParent(cursor)) {
            if (target.isAssignableFrom(cursor.getClass())) {
              result[tag] = (T) cursor;
              break;
            }
          }
          if (result[tag] == null) {
            throw new IllegalStateException("failed to find " + target.getSimpleName()
                + " from " + join + " on tag " + tag);
          }
        }
        return result;
      }

      protected SelectOperator replaceReduceSinkWithSelectOperator(ReduceSinkOperator childRS,
          ParseContext context) throws SemanticException {
        SelectOperator select = replaceOperatorWithSelect(childRS, context);
        select.getConf().setOutputColumnNames(childRS.getConf().getOutputValueColumnNames());
        select.getConf().setColList(childRS.getConf().getValueCols());
        return select;
      }

      protected SelectOperator replaceGroupbyWithSelectOperator(GroupByOperator childGby,
          ParseContext context) throws SemanticException {
        SelectOperator select = replaceOperatorWithSelect(childGby, context);
        ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
        for (String colName : childGby.getConf().getOutputColumnNames()) {
          colList.add(childGby.getColumnExprMap().get(colName));
        }
        select.getConf().setOutputColumnNames(childGby.getConf().getOutputColumnNames());
        select.getConf().setColList(colList);
        return select;
      }

      private SelectOperator replaceOperatorWithSelect(Operator<?> operator,
          ParseContext context) throws SemanticException {
        RowResolver inputRR = context.getOpParseCtx().get(operator).getRowResolver();
        SelectDesc select = new SelectDesc(null, null);

        Operator<?> parent = getSingleParent(operator);
        Operator<?> child = getSingleChild(operator);

        parent.getChildOperators().clear();

        SelectOperator sel = (SelectOperator) putOpInsertMap(
            OperatorFactory.getAndMakeChild(select, new RowSchema(inputRR
            .getColumnInfos()), parent), inputRR, context);

        sel.setColumnExprMap(operator.getColumnExprMap());

        sel.setChildOperators(operator.getChildOperators());
        for (Operator<? extends Serializable> ch : operator.getChildOperators()) {
          ch.replaceParent(operator, sel);
        }
        if (child instanceof ExtractOperator) {
          removeOperator(child, getSingleChild(child), sel, context);
        }
        operator.setChildOperators(null);
        operator.setParentOperators(null);
        return sel;
      }

      protected void removeReduceSinkForGroupBy(ReduceSinkOperator rs, GroupByOperator cgby,
          ParseContext context, int tag) throws SemanticException {

        Operator<?> parent = getSingleParent(rs);

        if (parent instanceof GroupByOperator) {
          GroupByOperator pgby = (GroupByOperator) parent;

          cgby.getConf().setKeys(pgby.getConf().getKeys());
          cgby.getConf().setAggregators(pgby.getConf().getAggregators());
          for (AggregationDesc aggr : pgby.getConf().getAggregators()) {
            aggr.setMode(GenericUDAFEvaluator.Mode.COMPLETE);
          }
          cgby.setColumnExprMap(pgby.getColumnExprMap());
          cgby.setSchema(pgby.getSchema());
          RowResolver resolver = context.getOpParseCtx().get(pgby).getRowResolver();
          context.getOpParseCtx().get(cgby).setRowResolver(resolver);
        } else {
          cgby.getConf().setKeys(backtrack(cgby.getConf().getKeys(), cgby, parent, tag));
          for (AggregationDesc aggr : cgby.getConf().getAggregators()) {
            aggr.setParameters(backtrack(aggr.getParameters(), cgby, parent, tag));
          }

          Map<String, ExprNodeDesc> oldMap = cgby.getColumnExprMap();
          RowResolver oldRR = context.getOpParseCtx().get(cgby).getRowResolver();

          Map<String, ExprNodeDesc> newMap = new HashMap<String, ExprNodeDesc>();
          RowResolver newRR = new RowResolver();

          List<String> outputCols = cgby.getConf().getOutputColumnNames();
          for (int i = 0; i < outputCols.size(); i++) {
            String colName = outputCols.get(i);
            String[] nm = oldRR.reverseLookup(colName);
            ColumnInfo colInfo = oldRR.get(nm[0], nm[1]);
            newRR.put(nm[0], nm[1], colInfo);
            ExprNodeDesc colExpr = backtrack(oldMap.get(colName), cgby, parent, 0);
            if (colExpr != null) {
              newMap.put(colInfo.getInternalName(), colExpr);
            }
          }
          cgby.setColumnExprMap(newMap);
          cgby.setSchema(new RowSchema(newRR.getColumnInfos()));
          context.getOpParseCtx().get(cgby).setRowResolver(newRR);
        }
        cgby.getConf().setMode(GroupByDesc.Mode.COMPLETE);

        removeOperator(rs, cgby, parent, context);

        if (parent instanceof GroupByOperator) {
          removeOperator(parent, cgby, getSingleParent(parent), context);
        }
      }

      protected GroupByOperator[] getPeerGroupbys(GroupByOperator gby) {
        Operator<?> parent = getSingleParent(gby);
        if (parent instanceof ForwardOperator) {
          GroupByOperator[] result = new GroupByOperator[parent.getChildOperators().size()];
          int index = 0;
          for (Operator<?> pgby : parent.getChildOperators()) {
            if (!(pgby instanceof GroupByOperator)) {
              return null;
            }
            result[index++] = (GroupByOperator) pgby;
          }
          return result;
        }
        return new GroupByOperator[] { gby };
      }

      protected void removeFollowingGroupBy(ReduceSinkOperator childRS, GroupByOperator cgby,
          ParseContext context) throws SemanticException {
        removeReduceSinkForGroupBy(childRS, cgby, context, 0);
        replaceGroupbyWithSelectOperator(cgby, context);
      }

      private void removeOperator(Operator<?> target, Operator<?> child, Operator<?> parent,
          ParseContext context) {
        for (Operator<? extends Serializable> aparent : target.getParentOperators()) {
          aparent.replaceChild(target, child);
        }
        for (Operator<? extends Serializable> achild : target.getChildOperators()) {
          achild.replaceParent(target, parent);
        }
        target.setChildOperators(null);
        target.setParentOperators(null);
        context.getOpParseCtx().remove(target);
      }

      private Operator<? extends Serializable> putOpInsertMap(Operator<?> op, RowResolver rr,
          ParseContext context) {
        OpParseContext ctx = new OpParseContext(rr);
        context.getOpParseCtx().put(op, ctx);
        return op;
      }
    }

    static class ReducerGroupbyProc extends AbsctractReducerReducerProc {

      public Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException {

        if (context.getConf().getBoolVar(HIVEGROUPBYSKEW)) {
          return false;
        }

        GroupByOperator cgby = (GroupByOperator)nd;
        ReduceSinkOperator childRS = (ReduceSinkOperator) getSingleParent(cgby);
        if (childRS == null) {
          return false;
        }
        Operator<? extends Serializable> check = getSingleParent(childRS);
        Operator<?> start = check instanceof GroupByOperator ? check : childRS;
        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
        ReduceSinkOperator[] possibleRS =
            findParentOperators(start, ReduceSinkOperator.class, trustScript);
        if (possibleRS == null || possibleRS.length != 1) {
          return false;
        }
        ReduceSinkOperator parentRS = possibleRS[0];
        if (merge(childRS, parentRS)) {
          removeReduceSinkForGroupBy(childRS, cgby, context, 0);
          dedupCtx.addRemovedOperator(childRS);
          return true;
        }
        return false;
      }
    }

    static class GroupbyReducerProc extends AbsctractReducerReducerProc {

      public Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException {

        if (context.getConf().getBoolVar(HIVEGROUPBYSKEW)) {
          return false;
        }

        ReduceSinkOperator childRS = (ReduceSinkOperator)nd;
        Operator<?> child = getSingleChild(childRS, true);
        if (child == null || child instanceof JoinOperator || child instanceof GroupByOperator) {
          return false;
        }
        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
        GroupByOperator[] possibleGBY =
            findParentOperators(childRS, GroupByOperator.class, trustScript);
        if (possibleGBY == null || possibleGBY.length != 1) {
          return false;
        }
        ReduceSinkOperator[] possibleParentRS = findParentOperators(possibleGBY[0],
            ReduceSinkOperator.class, trustScript);
        if (possibleParentRS == null || possibleParentRS.length != 1) {
          return false;
        }
        ReduceSinkOperator parentRS = possibleParentRS[0];
        if (merge(childRS, parentRS)) {
          replaceReduceSinkWithSelectOperator(childRS, context);
          dedupCtx.addRemovedOperator(childRS);
          return true;
        }
        return false;
      }
    }

    static class GroupbyGroupbyProc extends AbsctractReducerReducerProc {

      public Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException {

        if (context.getConf().getBoolVar(HIVEGROUPBYSKEW)) {
          return false;
        }

        GroupByOperator cgby = (GroupByOperator)nd;
        ReduceSinkOperator childRS = (ReduceSinkOperator) getSingleParent(cgby);
        if (childRS == null) {
          return false;
        }
        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
        Operator<? extends Serializable> check = getSingleParent(childRS);
        Operator<?> start = check instanceof GroupByOperator ? check : childRS;
        GroupByOperator[] possibleGBY =
            findParentOperators(start, GroupByOperator.class, trustScript);
        if (possibleGBY == null || possibleGBY.length != 1) {
          return false;
        }
        GroupByOperator pgby = possibleGBY[0];
        Operator<?> parent = getSingleParent(pgby);
        if (!(parent instanceof ReduceSinkOperator)) {
          return false;
        }
        ReduceSinkOperator parentRS = (ReduceSinkOperator) parent;
        if (merge(childRS, parentRS)) {
          removeFollowingGroupBy(childRS, cgby, context);
          dedupCtx.addRemovedOperator(childRS);
          dedupCtx.addRemovedOperator(cgby);
          return true;
        }
        return false;
      }
    }

    static class JoinReducerProc extends AbsctractReducerReducerProc {

      public Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException {

        if (context.getConf().getBoolVar(HIVEGROUPBYSKEW) ||
            context.getConf().getBoolVar(HIVESKEWJOIN)) {
          return false;
        }
        ReduceSinkOperator childRS = (ReduceSinkOperator) nd;
        Operator<?> child = getSingleChild(childRS);
        if (child instanceof JoinOperator || child instanceof GroupByOperator) {
          return false;
        }

        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
        ReduceSinkOperator[] possibleRSs =
            findParentOperators(childRS, ReduceSinkOperator.class, trustScript);
        for (int tag = 0; tag < possibleRSs.length; tag++) {
          ReduceSinkOperator parentRS = possibleRSs[tag];
          if (merge(childRS, parentRS, tag)) {
            replaceReduceSinkWithSelectOperator(childRS, context);
            dedupCtx.addRemovedOperator(childRS);
            return true;
          }
        }
        return false;
      }
    }

    static class JoinGroupbyProc extends AbsctractReducerReducerProc {

      public Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException {

        if (context.getConf().getBoolVar(HIVEGROUPBYSKEW) ||
            context.getConf().getBoolVar(HIVESKEWJOIN)) {
          return false;
        }

        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);

        GroupByOperator cgby = (GroupByOperator)nd;
        ReduceSinkOperator childRS = (ReduceSinkOperator) getSingleParent(cgby);

        Operator<? extends Serializable> check = getSingleParent(childRS);
        Operator<?> start = check instanceof GroupByOperator ? check : childRS;
        ReduceSinkOperator[] possibleRSs =
            findParentOperators(start, ReduceSinkOperator.class, trustScript);

        for (int tag = 0; tag < possibleRSs.length; tag++) {
          ReduceSinkOperator parentRS = possibleRSs[tag];
          if (merge(childRS, parentRS, tag)) {
            removeReduceSinkForGroupBy(childRS, cgby, context, tag);
            dedupCtx.addRemovedOperator(childRS);
            return true;
          }
        }
        return false;
      }
    }

    static class ReducerReducerProc extends AbsctractReducerReducerProc {

      public Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException {

        if (context.getConf().getBoolVar(HIVEGROUPBYSKEW)) {
          return false;
        }
        ReduceSinkOperator childRS = (ReduceSinkOperator) nd;
        Operator<?> child = getSingleChild(childRS);
        if (child instanceof JoinOperator || child instanceof GroupByOperator) {
          return false;
        }

        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
        ReduceSinkOperator[] possibleRSs =
            findParentOperators(childRS, ReduceSinkOperator.class, trustScript);
        if (possibleRSs == null || possibleRSs.length != 1) {
          return false;
        }
        ReduceSinkOperator parentRS = possibleRSs[0];
        child = getSingleChild(parentRS);
        if (child instanceof JoinOperator || child instanceof GroupByOperator) {
          return false;
        }
        if (merge(childRS, parentRS)) {
          replaceReduceSinkWithSelectOperator(childRS, context);
          dedupCtx.addRemovedOperator(childRS);
          return true;
        }
        return false;
      }
    }
  }
}
