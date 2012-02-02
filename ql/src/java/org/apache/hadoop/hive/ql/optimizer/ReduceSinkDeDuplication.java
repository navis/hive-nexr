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
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEGROUPBYSKEW;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESCRIPTOPERATORTRUST;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESKEWJOIN;
import static org.apache.hadoop.hive.ql.plan.JoinDesc.FULL_OUTER_JOIN;
import static org.apache.hadoop.hive.ql.plan.JoinDesc.INNER_JOIN;
import static org.apache.hadoop.hive.ql.plan.JoinDesc.LEFT_SEMI_JOIN;
import static org.apache.hadoop.hive.ql.plan.JoinDesc.RIGHT_OUTER_JOIN;

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
        .getJoinGroupbyProc());
    opRules.put(new RuleRegExp("R6", "RS%.*RS%JOIN%"), ReduceSinkDeduplicateProcFactory
        .getReducerJoinProc());

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

    public static NodeProcessor getJoinGroupbyProc() {
      return new JoinGroupbyProc();
    }

    public static NodeProcessor getReducerJoinProc() {
      return new ReducerJoinProc();
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
      
      protected boolean mergeToChild(ReduceSinkOperator childRS, ReduceSinkOperator parentRS) {
        return mergeToChild(childRS, parentRS, -1);
      }

      protected boolean mergeToChild(ReduceSinkOperator childRS, ReduceSinkOperator parentRS, 
          int index) {
        int[] result = checkAll(childRS, parentRS, index);
        if (result == null) {
          return false;
        }
        if (result[0] > 0) {
          Operator<?> terminal = getSingleParent(parentRS);
          ArrayList<ExprNodeDesc> childKCs = childRS.getConf().getKeyCols();
          parentRS.getConf().setKeyCols(backtrack(childKCs, childRS, terminal, index));
        }
        if (result[1] > 0) {
          Operator<?> terminal = getSingleParent(parentRS);
          ArrayList<ExprNodeDesc> childPCs = childRS.getConf().getPartitionCols();
          parentRS.getConf().setPartitionCols(backtrack(childPCs, childRS, terminal, index));
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
      protected int[] checkAll(ReduceSinkOperator childRS, ReduceSinkOperator parentRS,
          int index) {
        ReduceSinkDesc cconf = childRS.getConf();
        ReduceSinkDesc pconf = parentRS.getConf();

        List<ExprNodeDesc> ckeys = cconf.getKeyCols();
        List<ExprNodeDesc> pkeys = pconf.getKeyCols();
        Integer moveKeyColTo = checkExprs(ckeys, pkeys, childRS, parentRS, index);
        if (moveKeyColTo == null) {
          return null;
        }
        List<ExprNodeDesc> cpars = cconf.getPartitionCols();
        List<ExprNodeDesc> ppars = pconf.getPartitionCols();
        Integer movePartitionColTo = checkExprs(cpars, ppars, childRS, parentRS, index);
        if (movePartitionColTo == null) {
          return null;
        }
        String corder = cconf.getOrder();
        String porder = pconf.getOrder();
        Integer moveRSOrderTo = checkOrder(corder, porder);
        if (moveRSOrderTo == null) {
          return null;
        }
        int creduce = cconf.getNumReducers();
        int preduce = pconf.getNumReducers();
        Integer moveReducerNumTo = checkNumReducer(creduce, preduce);
        if (moveReducerNumTo == null) {
          return null;
        }
        return new int[] {moveKeyColTo, movePartitionColTo, moveRSOrderTo, moveReducerNumTo};
      }

      protected int[] checkMisc(ReduceSinkOperator childRS, ReduceSinkOperator parentRS) {
        ReduceSinkDesc cconf = childRS.getConf();
        ReduceSinkDesc pconf = parentRS.getConf();

        String corder = cconf.getOrder();
        String porder = pconf.getOrder();
        Integer moveRSOrderTo = checkOrder(corder, porder);
        if (moveRSOrderTo == null) {
          return null;
        }
        int creduce = cconf.getNumReducers();
        int preduce = pconf.getNumReducers();
        Integer moveReducerNumTo = checkNumReducer(creduce, preduce);
        if (moveReducerNumTo == null) {
          return null;
        }
        return new int[] {moveRSOrderTo, moveReducerNumTo};
      }

      private Integer checkOrder(String corder, String porder) {
        Integer moveRSOrderTo = 0;
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
              moveRSOrderTo = null;
            } else {
              moveRSOrderTo = Integer.valueOf(corder.length()).compareTo(porder.length());
            }
          }
        }
        return moveRSOrderTo;
      }

      private Integer checkNumReducer(int creduce, int preduce) {
        Integer moveReducerNumTo = 0;
        if (creduce < 0) {
          if (preduce >= 0) {
            moveReducerNumTo = -1;
          }
        } else {
          if (preduce < 0) {
            moveReducerNumTo = 1;
          } else {
            if (creduce != preduce) {
              moveReducerNumTo = null;
            }
          }
        }
        return moveReducerNumTo;
      }

      protected Integer checkExprs(List<ExprNodeDesc> cexprs, List<ExprNodeDesc> pexprs,
          Operator<?> childOP, Operator<?> parentOP, int index) {
        Integer moveKeyColTo = 0;
        if (cexprs == null || cexprs.isEmpty()) {
          if (pexprs != null && !pexprs.isEmpty()) {
            Operator<?> terminal = getSingleParent(parentOP);
            for (ExprNodeDesc pkey : pexprs) {
              if (backtrack(pkey, parentOP, terminal) == null) {
                return null;
              }
            }
            moveKeyColTo = -1;
          }
        } else {
          if (pexprs == null || pexprs.isEmpty()) {
            Operator<?> terminal = getSingleParent(parentOP);
            for (ExprNodeDesc ckey : cexprs) {
              if (backtrack(ckey, childOP, terminal) == null) {
                return null;
              }
            }
            moveKeyColTo = 1;
          } else {
            moveKeyColTo = sameKeys(cexprs, pexprs, childOP, parentOP, index);
          }
        }
        return moveKeyColTo;
      }

      protected Integer sameKeys(List<ExprNodeDesc> cexprs, List<ExprNodeDesc> pexprs,
          Operator<?> child, Operator<?> parent, int index) {
        int common = Math.min(cexprs.size(), pexprs.size());
        int limit = Math.max(cexprs.size(), pexprs.size());
        Operator<?> terminal = getSingleParent(parent);
        int i = 0;
        for (; i < common; i++) {
          ExprNodeDesc cexpr = backtrack(cexprs.get(i), child, terminal, index);
          ExprNodeDesc pexpr = backtrack(pexprs.get(i), parent, terminal, index);
          if (cexpr == null || !cexpr.isSame(pexpr)) {
            return null;
          }
        }
        for (;i < limit; i++) {
          if (cexprs.size() > pexprs.size()) {
            if (backtrack(cexprs.get(i), child, terminal, index) == null) {
              return null;
            }
          } else if (backtrack(pexprs.get(i), parent, terminal, index) == null) {
            return null;
          }
        }
        return Integer.valueOf(cexprs.size()).compareTo(pexprs.size());
      }

      protected ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources,
          Operator<?> current, Operator<?> terminal) {
        return backtrack(sources, current, terminal, -1);
      }

      protected ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources,
          Operator<?> current, Operator<?> terminal, int index) {
        ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>();
        for (ExprNodeDesc expr : sources) {
          result.add(backtrack(expr, current, terminal, index));
        }
        return result;
      }

      protected ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current,
          Operator<?> terminal) {
        return backtrack(source, current, terminal, -1);
      }

      protected ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current,
          Operator<?> terminal, int index) {
        if (source instanceof ExprNodeGenericFuncDesc) {
          List<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
          for (ExprNodeDesc param : source.getChildren()) {
            ExprNodeDesc backtrack = backtrack(param, current, terminal, index);
            if (backtrack == null) {
              return null;
            }
            params.add(backtrack);
          }
          ExprNodeGenericFuncDesc function = (ExprNodeGenericFuncDesc) source.clone();
          function.setChildExprs(params);
          return function;
        }
        if (source instanceof ExprNodeColumnDesc) {
          ExprNodeColumnDesc column = (ExprNodeColumnDesc) source;
          return backtrack(column, current, terminal, index);
        }
        if (source instanceof ExprNodeFieldDesc) {
          ExprNodeFieldDesc field = (ExprNodeFieldDesc) source;
          String name = field.getFieldName();
          TypeInfo type = field.getTypeInfo();
          ExprNodeDesc backtrack = backtrack(field.getDesc(), current, terminal, index);
          return new ExprNodeFieldDesc(type, backtrack, name, field.getIsList());
        }
        return source;
      }

      private ExprNodeDesc backtrack(ExprNodeColumnDesc column, Operator<?> current,
          Operator<?> terminal, int index) {
        if (current == null || current == terminal) {
          return column;
        }
        if (current instanceof JoinOperator) {
          JoinDesc joinconf = ((JoinOperator)current).getConf();
          Byte tag = joinconf.getTagOrder()[index];
          if (!joinconf.getReversedExprs().get(column.getColumn()).equals(tag)) {
            return null;
          }
        }
        Map<String, ExprNodeDesc> mapping = current.getColumnExprMap();
        if (mapping == null || !mapping.containsKey(column.getColumn())) {
          return backtrack(column, getSingleParent(current, index), terminal, index);
        }
        ExprNodeDesc mapped = mapping.get(column.getColumn());
        return backtrack(mapped, getSingleParent(current, index), terminal, index);
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

      protected Operator<?> getSingleParent(Operator<?> operator, int index) {
        if (operator instanceof JoinOperator && index < 0) {
          throw new IllegalStateException("invald walk");
        }
        return operator instanceof JoinOperator ? operator.getParentOperators().get(index)
            : getSingleParent(operator);
      }

      @SuppressWarnings("unchecked")
      protected <T extends Operator<?>> T[] findParentOperators(Operator<?> start,
          Class<T> target, boolean trustScript) {
        if (start instanceof JoinOperator) {
          return findParentOperators((JoinOperator) start, target);
        }
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
              || cursor instanceof GroupByOperator
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
          ParseContext context) throws SemanticException {

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
          cgby.getConf().setKeys(backtrack(cgby.getConf().getKeys(), cgby, parent));
          for (AggregationDesc aggr : cgby.getConf().getAggregators()) {
            aggr.setParameters(backtrack(aggr.getParameters(), cgby, parent));
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
        removeReduceSinkForGroupBy(childRS, cgby, context);
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
        if (mergeToChild(childRS, parentRS)) {
          removeReduceSinkForGroupBy(childRS, cgby, context);
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
        if (mergeToChild(childRS, parentRS)) {
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
        if (mergeToChild(childRS, parentRS)) {
          removeFollowingGroupBy(childRS, cgby, context);
          dedupCtx.addRemovedOperator(childRS);
          dedupCtx.addRemovedOperator(cgby);
          return true;
        }
        return false;
      }
    }

    static abstract class AbstractJoinXProc extends AbsctractReducerReducerProc {

      public Object process(Node nd, Stack<Node> stack, ParseContext context)
          throws SemanticException {

        if (context.getConf().getBoolVar(HIVEGROUPBYSKEW) ||
            context.getConf().getBoolVar(HIVESKEWJOIN)) {
          return false;
        }
        ReduceSinkOperator childRS = checkChild(nd);
        if (childRS == null) {
          return false;
        }
        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
        JoinOperator[] joins =
            findParentOperators(childRS, JoinOperator.class, trustScript);
        if (joins == null || joins.length != 1) {
          return false;
        }
        ReduceSinkOperator[] parentRSs =
            findParentOperators(joins[0], ReduceSinkOperator.class, trustScript);
        if (parentRSs == null || parentRSs.length == 0) {
          return false;
        }
        // cross join
        if (parentRSs[0].getConf().getKeyCols().isEmpty()) {
          return false;
        }
        // order, reducer num
        int[] misc = checkMisc(childRS, parentRSs[0]);
        if (misc == null) {
          return false;
        }
        // partition cannot be moved
        List<ExprNodeDesc> cpars = childRS.getConf().getPartitionCols();
        List<ExprNodeDesc> ppars = parentRSs[0].getConf().getPartitionCols();
          Integer result = checkExprs(cpars, ppars, childRS, parentRSs[0], 0);
        if (result == null || result != 0) {
          return null;
        }
        Set<Byte> sorted = sortedTags(joins[0]);
        Map<Byte, List<Pair>> mapped = keyMapping(childRS, parentRSs, sorted);
        if (mapped == null) {
          return false;
        }

        for (Map.Entry<Byte, List<Pair>> entry : mapped.entrySet()) {
          ReduceSinkDesc pconf = parentRSs[entry.getKey()].getConf();
          ArrayList<ExprNodeDesc> newKeys = new ArrayList<ExprNodeDesc>(pconf.getKeyCols());
          String order = pconf.getOrder();
          for  (int i = order.length(); i < pconf.getKeyCols().size(); i++) {
            order += '+';
          }
          for (Pair expr : entry.getValue()) {
            newKeys.add(expr.expr);
            order += expr.asc ? '+' : '-';
          }
          pconf.setKeyCols(newKeys);
          if (!pconf.getOrder().isEmpty() || order.contains("-")) {
            pconf.setOrder(order);
          }
        }

        for (int index = 0; index < parentRSs.length; index++) {
          if (misc[1] > 0) {
            parentRSs[index].getConf().setNumReducers(childRS.getConf().getNumReducers());
          }
        }
        return dedup(context, childRS);
      }

      protected abstract ReduceSinkOperator checkChild(Node child);

      protected abstract boolean dedup(ParseContext context, ReduceSinkOperator childRS) throws SemanticException;

      private Map<Byte, List<Pair>> keyMapping(ReduceSinkOperator childRS,
          ReduceSinkOperator[] parentRSs, Set<Byte> sorted) {

        String order = childRS.getConf().getOrder();
        List<ExprNodeDesc> keys = childRS.getConf().getKeyCols();

        List<Pair> exprs = new ArrayList<Pair>();
        for (int i = 0; i < keys.size(); i++) {
          exprs.add(new Pair(keys.get(i), order.length() < i || order.charAt(i) == '+'));
        }

        Map<Byte, List<Pair>> mapped = new HashMap<Byte, List<Pair>>();
        for (int i = 0; i < parentRSs.length && !exprs.isEmpty(); i++) {
          ReduceSinkOperator parentRS = parentRSs[i];
          byte tag = (byte) parentRS.getConf().getTag();
          Operator<?> terminal = getSingleParent(parentRS);

          ArrayList<ExprNodeDesc> keyCols = parentRS.getConf().getKeyCols();
          List<ExprNodeDesc> pkeys = backtrack(keyCols, parentRS, terminal, tag);
          List<Pair> removed = new ArrayList<Pair>();
          for (Pair expr : exprs) {
            ExprNodeDesc backtrack = backtrack(expr.expr, childRS, terminal, tag);
            if (backtrack == null) {
              continue;
            }
            removed.add(expr);
            if (!sorted.contains(tag)) {
              return null;
            }
            boolean keyPart = false;
            Iterator<ExprNodeDesc> it = pkeys.iterator();
            while (it.hasNext()) {
               if (it.next().isSame(backtrack)) {
                 keyPart = true;
                 it.remove();
                 break;
               }
            }
            if (keyPart) {
              continue;
            }
            if (!pkeys.isEmpty()) {
              return null;
            }
            List<Pair> list = mapped.get(tag);
            if (list == null) {
              mapped.put(tag, list = new ArrayList<Pair>());
            }
            list.add(new Pair(backtrack, expr.asc));
          }
          exprs.removeAll(removed);
          removed.clear();
        }
        return exprs.isEmpty() ? mapped : null;
      }

      // from JoinPPD#getQualifiedAliases
      private Set<Byte> sortedTags(JoinOperator op) {
        Set<Byte> sorted = new HashSet<Byte>();

        Byte[] orders = op.getConf().getTagOrder();
        JoinCondDesc[] conds = op.getConf().getConds();
        int i;
        for (i = conds.length - 1; i >= 0; i--) {
          int type = conds[i].getType();
          if (type == INNER_JOIN || type == LEFT_SEMI_JOIN) {
            sorted.add(orders[conds[i].getRight()]);
          } else if (type == RIGHT_OUTER_JOIN) {
            sorted.add(orders[conds[i].getRight()]);
            break;
          } else if (type == FULL_OUTER_JOIN) {
            break;
          }
        }
        if (i == -1) {
          sorted.add(orders[conds[0].getLeft()]);
        }
        return sorted;
      }

      private static class Pair {
        ExprNodeDesc expr;
        boolean asc;
        Pair(ExprNodeDesc expr, boolean asc) {
          this.expr = expr;
          this.asc = asc;
        }
      }
    }

    static class JoinReducerProc extends AbstractJoinXProc {

      protected ReduceSinkOperator checkChild(Node nd) {
        ReduceSinkOperator childRS = (ReduceSinkOperator) nd;
        Operator<?> child = getSingleChild(childRS);
        if (child instanceof JoinOperator || child instanceof GroupByOperator) {
          return null;
        }
        return childRS;
      }

      protected boolean dedup(ParseContext context, ReduceSinkOperator childRS) throws SemanticException {
        replaceReduceSinkWithSelectOperator(childRS, context);
        dedupCtx.addRemovedOperator(childRS);
        return true;
      }
    }

    static class JoinGroupbyProc extends AbstractJoinXProc {

      protected ReduceSinkOperator checkChild(Node nd) {
        GroupByOperator cgby = (GroupByOperator)nd;
        return (ReduceSinkOperator) getSingleParent(cgby);
      }

      protected boolean dedup(ParseContext context, ReduceSinkOperator childRS) throws SemanticException {
        GroupByOperator cgby = (GroupByOperator) getSingleChild(childRS);
        removeReduceSinkForGroupBy(childRS, cgby, context);
        dedupCtx.addRemovedOperator(childRS);
        return true;
      }
    }

    static class ReducerJoinProc extends AbsctractReducerReducerProc {

      protected Object process(Node nd, Stack<Node> stack, ParseContext context) throws SemanticException {
        JoinOperator join = (JoinOperator) nd;
        ReduceSinkOperator childRS = (ReduceSinkOperator) stack.get(stack.size() - 2);
        int tag = join.getChildren().indexOf(childRS);

        boolean trustScript = context.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
        ReduceSinkOperator[] possibleRSs =
            findParentOperators(childRS, ReduceSinkOperator.class, trustScript);
        if (possibleRSs == null || possibleRSs.length != 1) {
          return false;
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
        if (mergeToChild(childRS, parentRS)) {
          if (parentRS.getConf().getKeyCols().isEmpty()) {
            return false;
          }
          replaceReduceSinkWithSelectOperator(childRS, context);
          dedupCtx.addRemovedOperator(childRS);
          return true;
        }
        return false;
      }
    }
  }
}
