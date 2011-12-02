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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEGROUPBYSKEW;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESCRIPTOPERATORTRUST;

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
    opRules.put(new RuleRegExp("R1", "RS%.*RS%GBY%"), ReduceSinkDeduplicateProcFactory
        .getReducerGbyReducerGbyProc());
    opRules.put(new RuleRegExp("R2", "RS%.*RS%"), ReduceSinkDeduplicateProcFactory
        .getReducerReducerProc());

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
    List<ReduceSinkOperator> rejectedRSList;

    public ReduceSinkDeduplicateProcCtx(ParseContext pctx) {
      rejectedRSList = new ArrayList<ReduceSinkOperator>();
      this.pctx = pctx;
    }
    
    public boolean contains (ReduceSinkOperator rsOp) {
      return rejectedRSList.contains(rsOp);
    }
    
    public void addRejectedReduceSinkOperator(ReduceSinkOperator rsOp) {
      if (!rejectedRSList.contains(rsOp)) {
        rejectedRSList.add(rsOp);
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


    public static NodeProcessor getReducerGbyReducerGbyProc() {
      return new ReducerGbyReducerGbyProc();
    }

    public static NodeProcessor getReducerReducerProc() {
      return new ReducerReducerProc();
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
    
    static class ReducerReducerProc implements NodeProcessor {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        ReduceSinkDeduplicateProcCtx ctx = (ReduceSinkDeduplicateProcCtx) procCtx;
        ReduceSinkOperator childReduceSink = (ReduceSinkOperator)nd;
        
        if(ctx.contains(childReduceSink)) {
          return null;
        }
        
        List<Operator<? extends Serializable>> childOp = childReduceSink.getChildOperators();
        if (childOp != null && childOp.size() == 1 && childOp.get(0) instanceof GroupByOperator) {
          ctx.addRejectedReduceSinkOperator(childReduceSink);
          return null;
        }

        ParseContext pGraphContext = ctx.getPctx();
        HashMap<String, String> childColumnMapping = getPartitionAndKeyColumnMapping(childReduceSink);
        ReduceSinkOperator parentRS = null;
        parentRS = findSingleParentReduceSink(childReduceSink, pGraphContext);
        if (parentRS == null) {
          ctx.addRejectedReduceSinkOperator(childReduceSink);
          return null;
        }
        HashMap<String, String> parentColumnMapping = getPartitionAndKeyColumnMapping(parentRS);
        Operator<? extends Serializable> stopBacktrackFlagOp = null;
        if (parentRS.getParentOperators() == null
            || parentRS.getParentOperators().size() == 0) {
          stopBacktrackFlagOp =  parentRS;
        } else if (parentRS.getParentOperators().size() != 1) {
          return null;
        } else {
          stopBacktrackFlagOp = parentRS.getParentOperators().get(0);
        }
        
        boolean succeed = backTrackColumnNames(childColumnMapping, childReduceSink, stopBacktrackFlagOp, pGraphContext);
        if (!succeed) {
          return null;
        }
        succeed = backTrackColumnNames(parentColumnMapping, parentRS, stopBacktrackFlagOp, pGraphContext);
        if (!succeed) {
          return null;
        }
        
        boolean same = compareReduceSink(childReduceSink, parentRS, childColumnMapping, parentColumnMapping);
        if (!same) {
          return null;
        }
        replaceReduceSinkWithSelectOperator(childReduceSink, pGraphContext);
        return null;
      }

      private void replaceReduceSinkWithSelectOperator(
          ReduceSinkOperator childReduceSink, ParseContext pGraphContext) throws SemanticException {
        List<Operator<? extends Serializable>> parentOp = childReduceSink.getParentOperators();
        List<Operator<? extends Serializable>> childOp = childReduceSink.getChildOperators();
        
        Operator<? extends Serializable> oldParent = childReduceSink;
        
        if (childOp != null && childOp.size() == 1
            && ((childOp.get(0)) instanceof ExtractOperator)) {
          oldParent = childOp.get(0);
          childOp = childOp.get(0).getChildOperators();
        }
        
        Operator<? extends Serializable> input = parentOp.get(0);
        input.getChildOperators().clear();
        
        RowResolver inputRR = pGraphContext.getOpParseCtx().get(input).getRowResolver();

        ArrayList<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>();
        ArrayList<String> outputs = new ArrayList<String>();
        List<String> outputCols = childReduceSink.getConf().getOutputValueColumnNames();
        RowResolver outputRS = new RowResolver();

        Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

        for (int i = 0; i < outputCols.size(); i++) {
          String internalName = outputCols.get(i);
          String[] nm = inputRR.reverseLookup(internalName);
          ColumnInfo valueInfo = inputRR.get(nm[0], nm[1]);
          ExprNodeDesc colDesc = childReduceSink.getConf().getValueCols().get(i);
          exprs.add(colDesc);
          outputs.add(internalName);
          outputRS.put(nm[0], nm[1], new ColumnInfo(internalName, valueInfo
              .getType(), nm[0], valueInfo.getIsVirtualCol(), valueInfo.isHiddenVirtualCol()));
          colExprMap.put(internalName, colDesc);
        }

        SelectDesc select = new SelectDesc(exprs, outputs, false);

        SelectOperator sel = (SelectOperator) putOpInsertMap(
            OperatorFactory.getAndMakeChild(select, new RowSchema(inputRR
            .getColumnInfos()), input), inputRR, pGraphContext);

        sel.setColumnExprMap(colExprMap);

        // Insert the select operator in between.
        sel.setChildOperators(childOp);
        for (Operator<? extends Serializable> ch : childOp) {
          ch.replaceParent(oldParent, sel);
        }
        
      }
      
      private Operator<? extends Serializable> putOpInsertMap(
          Operator<? extends Serializable> op, RowResolver rr, ParseContext pGraphContext) {
        OpParseContext ctx = new OpParseContext(rr);
        pGraphContext.getOpParseCtx().put(op, ctx);
        return op;
      }

      private boolean compareReduceSink(ReduceSinkOperator childReduceSink,
          ReduceSinkOperator parentRS,
          HashMap<String, String> childColumnMapping,
          HashMap<String, String> parentColumnMapping) {
        
        ArrayList<ExprNodeDesc> childPartitionCols = childReduceSink.getConf().getPartitionCols();
        ArrayList<ExprNodeDesc> parentPartitionCols = parentRS.getConf().getPartitionCols();
        
        boolean ret = compareExprNodes(childColumnMapping, parentColumnMapping,
            childPartitionCols, parentPartitionCols);
        if (!ret) {
          return false;
        }
        
        ArrayList<ExprNodeDesc> childReduceKeyCols = childReduceSink.getConf().getKeyCols();
        ArrayList<ExprNodeDesc> parentReduceKeyCols = parentRS.getConf().getKeyCols();
        ret = compareExprNodes(childColumnMapping, parentColumnMapping,
            childReduceKeyCols, parentReduceKeyCols);
        if (!ret) {
          return false;
        }
        
        String childRSOrder = childReduceSink.getConf().getOrder();
        String parentRSOrder = parentRS.getConf().getOrder();
        boolean moveChildRSOrderToParent = false;
        //move child reduce sink's order to the parent reduce sink operator.
        if (childRSOrder != null && !(childRSOrder.trim().equals(""))) {
          if (parentRSOrder == null
              || !childRSOrder.trim().equals(parentRSOrder.trim())) {
            return false;
          }
        } else {
          if(parentRSOrder == null || parentRSOrder.trim().equals("")) {
            moveChildRSOrderToParent = true;
          }
        }
        
        int childNumReducers = childReduceSink.getConf().getNumReducers();
        int parentNumReducers = parentRS.getConf().getNumReducers();
        boolean moveChildReducerNumToParent = false;
        //move child reduce sink's number reducers to the parent reduce sink operator.
        if (childNumReducers != parentNumReducers) {
          if (childNumReducers == -1) {
            //do nothing. 
          } else if (parentNumReducers == -1) {
            //set childNumReducers in the parent reduce sink operator.
            moveChildReducerNumToParent = true;
          } else {
            return false;
          }
        }
        
        if(moveChildRSOrderToParent) {
          parentRS.getConf().setOrder(childRSOrder);          
        }
        
        if(moveChildReducerNumToParent) {
          parentRS.getConf().setNumReducers(childNumReducers);
        }
        
        return true;
      }

      private boolean compareExprNodes(HashMap<String, String> childColumnMapping,
          HashMap<String, String> parentColumnMapping,
          ArrayList<ExprNodeDesc> childColExprs,
          ArrayList<ExprNodeDesc> parentColExprs) {
        
        boolean childEmpty = childColExprs == null || childColExprs.size() == 0;
        boolean parentEmpty = parentColExprs == null || parentColExprs.size() == 0;
        
        if (childEmpty) { //both empty
          return true;
        }
        
        //child not empty here
        if (parentEmpty) { // child not empty, but parent empty
          return false;
        }

        if (childColExprs.size() != parentColExprs.size()) {
          return false;
        }
        int i = 0;
        while (i < childColExprs.size()) {
          ExprNodeDesc childExpr = childColExprs.get(i);
          ExprNodeDesc parentExpr = parentColExprs.get(i);

          if ((childExpr instanceof ExprNodeColumnDesc)
              && (parentExpr instanceof ExprNodeColumnDesc)) {
            String childCol = childColumnMapping
                .get(((ExprNodeColumnDesc) childExpr).getColumn());
            String parentCol = parentColumnMapping
                .get(((ExprNodeColumnDesc) childExpr).getColumn());

            if (!childCol.equals(parentCol)) {
              return false;
            }
          } else {
            return false;
          }
          i++;
        }
        return true;
      }

      /*
       * back track column names to find their corresponding original column
       * names. Only allow simple operators like 'select column' or filter.
       */
      private boolean backTrackColumnNames(
          HashMap<String, String> columnMapping,
          ReduceSinkOperator reduceSink,
          Operator<? extends Serializable> stopBacktrackFlagOp, ParseContext pGraphContext) {
        Operator<? extends Serializable> startOperator = reduceSink;
        while (startOperator != null && startOperator != stopBacktrackFlagOp) {
          startOperator = startOperator.getParentOperators().get(0);
          Map<String, ExprNodeDesc> colExprMap = startOperator.getColumnExprMap();
          if(colExprMap == null || colExprMap.size()==0) {
            continue;
          }
          Iterator<String> keyIter = columnMapping.keySet().iterator();
          while (keyIter.hasNext()) {
            String key = keyIter.next();
            String oldCol = columnMapping.get(key);
            ExprNodeDesc exprNode = colExprMap.get(oldCol);
            if(exprNode instanceof ExprNodeColumnDesc) {
              String col = ((ExprNodeColumnDesc)exprNode).getColumn();
              columnMapping.put(key, col);
            } else {
              return false;
            }
          }
        }
        
        return true;
      }

      private HashMap<String, String> getPartitionAndKeyColumnMapping(ReduceSinkOperator reduceSink) {
        HashMap<String, String> columnMapping = new HashMap<String, String> ();
        ReduceSinkDesc reduceSinkDesc = reduceSink.getConf();
        ArrayList<ExprNodeDesc> partitionCols = reduceSinkDesc.getPartitionCols();
        ArrayList<ExprNodeDesc> reduceKeyCols = reduceSinkDesc.getKeyCols();
        if(partitionCols != null) {
          for (ExprNodeDesc desc : partitionCols) {
            List<String> cols = desc.getCols();
            for(String col : cols) {
              columnMapping.put(col, col);
            }
          }
        }
        if(reduceKeyCols != null) {
          for (ExprNodeDesc desc : reduceKeyCols) {
            List<String> cols = desc.getCols();
            for(String col : cols) {
              columnMapping.put(col, col);
            }
          }
        }
        return columnMapping;
      }

      private ReduceSinkOperator findSingleParentReduceSink(ReduceSinkOperator childReduceSink, ParseContext pGraphContext) {
        Operator<? extends Serializable> start = childReduceSink;
        while(start != null) {
          if (start.getParentOperators() == null
              || start.getParentOperators().size() != 1) {
            // this potentially is a join operator
            return null;
          }
          
          boolean allowed = false;
          if ((start instanceof SelectOperator)
              || (start instanceof FilterOperator)
              || (start instanceof ExtractOperator)
              || (start instanceof ForwardOperator)
              || (start instanceof ScriptOperator)
              || (start instanceof ReduceSinkOperator)) {
            allowed = true;
          }
          
          if (!allowed) {
            return null;
          }
          
          if ((start instanceof ScriptOperator)
              && !HiveConf.getBoolVar(pGraphContext.getConf(),
                  HiveConf.ConfVars.HIVESCRIPTOPERATORTRUST)) {
            return null;
          }
          
          start = start.getParentOperators().get(0);
          if(start instanceof ReduceSinkOperator) {
            return (ReduceSinkOperator)start;
          }
        }
        return null;
      }
    }
    
    static class ReducerGbyReducerGbyProc implements NodeProcessor {

      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        ReduceSinkDeduplicateProcCtx ctx = (ReduceSinkDeduplicateProcCtx) procCtx;
        ParseContext pGraphContext = ctx.getPctx();

        if (pGraphContext.getConf().getBoolVar(HIVEGROUPBYSKEW)) {
          return false;
        }

        boolean trustScript = pGraphContext.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);

        GroupByOperator childGroupBy = (GroupByOperator)nd;
        ReduceSinkOperator childRS = (ReduceSinkOperator) getSingleParent(childGroupBy);
        if (childRS == null) {
          return false;
        }

        ReduceSinkOperator parentRS = findPossibleReduceSink(getSingleParent(childRS), trustScript);
        if (parentRS == null) {
          return false;
        }
        List<ExprNodeDesc> ckeys = childRS.getConf().getKeyCols();
        List<ExprNodeDesc> pkeys = parentRS.getConf().getKeyCols();
        if (pkeys != null && !pkeys.isEmpty() && !sameKeys(ckeys, pkeys, childRS, parentRS)) {
          return false;
        }
        List<ExprNodeDesc> cpars = childRS.getConf().getPartitionCols();
        List<ExprNodeDesc> ppars = parentRS.getConf().getPartitionCols();
        if (ppars != null && !ppars.isEmpty() && !sameKeys(cpars, ppars, childRS, parentRS)) {
          return false;
        }
        String corder = childRS.getConf().getOrder();
        String porder = parentRS.getConf().getOrder();
        if (corder != null && !corder.trim().equals("")) {
          if (porder == null || !corder.trim().equals(porder.trim())) {
            return false;
          }
        } else if (porder == null || porder.trim().equals("")) {
          parentRS.getConf().setOrder(corder);
        }
        int creduce = childRS.getConf().getNumReducers();
        int preduce = parentRS.getConf().getNumReducers();
        if (creduce != preduce) {
          if (creduce >= 0 && preduce >= 0) {
            return false;
          }
          if (preduce == -1) {
            parentRS.getConf().setNumReducers(creduce);
          }
        }

        replaceReduceSinkWithSelectOperator(parentRS, pGraphContext);
        return true;
      }

      private boolean sameKeys(List<ExprNodeDesc> einits, List<ExprNodeDesc> eterms, Operator<?> init, Operator<?> terminal) {
        if (eterms == null || eterms.size() != einits.size()) {
          return false;
        }
        for (int i = 0; i < einits.size(); i++) {
          if (!backtrack(einits.get(i), init, terminal).isSame(eterms.get(i))) {
            return false;
          }
        }
        return true;
      }

      private ArrayList<ExprNodeDesc> backtrack(List<ExprNodeDesc> sources, Operator<?> current, Operator<?> terminal) {
        ArrayList<ExprNodeDesc> result = new ArrayList<ExprNodeDesc>();
        for (ExprNodeDesc expr : sources) {
          result.add(backtrack(expr, current, terminal));
        }
        return result;
      }

      private ExprNodeDesc backtrack(ExprNodeDesc source, Operator<?> current, Operator<?> terminal) {
        if (current == terminal) {
          return source;
        }
        if (source instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc function = (ExprNodeGenericFuncDesc) source.clone();
          ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
          for (ExprNodeDesc param : function.getChildren()) {
            params.add(backtrack(param, current, terminal));
          }
          function.setChildExprs(params);
          return function;
        }
        if (source instanceof ExprNodeColumnDesc) {
          ExprNodeColumnDesc column = (ExprNodeColumnDesc) source;
          return backtrack(column, current, terminal);
        }
        if (source instanceof ExprNodeFieldDesc) {
          ExprNodeFieldDesc field = (ExprNodeFieldDesc) source;
          return new ExprNodeFieldDesc(field.getTypeInfo(), backtrack(field.getDesc(), current, terminal), field.getFieldName(), field.getIsList());
        }
        return source;
      }

      private ExprNodeDesc backtrack(ExprNodeColumnDesc column, Operator<?> current, Operator<?> terminal) {
        if (current == null || current == terminal) {
          return column;
        }
        Map<String, ExprNodeDesc> mapping = current.getColumnExprMap();
        if (mapping == null || !mapping.containsKey(column.getColumn())) {
          return backtrack(column, getSingleParent(current), terminal);
        }
        return backtrack(mapping.get(column.getColumn()), getSingleParent(current), terminal);
      }

      private Operator<? extends Serializable> getSingleParent(Operator<? extends Serializable> operator) {
        if (operator.getParentOperators() != null && operator.getParentOperators().size() == 1) {
          return operator.getParentOperators().get(0);
        }
        return null;
      }

      private ReduceSinkOperator findPossibleReduceSink(Operator<?> start, boolean trustScript) {
        Operator<?> cursor = getSingleParent(start);
        for (; cursor != null; cursor = getSingleParent(cursor)) {
          if (ReduceSinkOperator.class.isAssignableFrom(cursor.getClass())) {
            return (ReduceSinkOperator)cursor;
          }
          if (!trustScript && cursor instanceof ScriptOperator) {
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

      // copied from ReducerReducerProc
      private void replaceReduceSinkWithSelectOperator(
          ReduceSinkOperator childReduceSink, ParseContext pGraphContext) throws SemanticException {
        List<Operator<? extends Serializable>> parentOp = childReduceSink.getParentOperators();
        List<Operator<? extends Serializable>> childOp = childReduceSink.getChildOperators();

        Operator<? extends Serializable> oldParent = childReduceSink;

        if (childOp != null && childOp.size() == 1
            && ((childOp.get(0)) instanceof ExtractOperator)) {
          oldParent = childOp.get(0);
          childOp = childOp.get(0).getChildOperators();
        }

        Operator<? extends Serializable> input = parentOp.get(0);
        input.getChildOperators().clear();

        RowResolver inputRR = pGraphContext.getOpParseCtx().get(input).getRowResolver();

        ArrayList<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>();
        ArrayList<String> outputs = new ArrayList<String>();
        List<String> outputCols = childReduceSink.getConf().getOutputValueColumnNames();
        RowResolver outputRS = new RowResolver();

        Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

        for (int i = 0; i < outputCols.size(); i++) {
          String internalName = outputCols.get(i);
          String[] nm = inputRR.reverseLookup(internalName);
          ColumnInfo valueInfo = inputRR.get(nm[0], nm[1]);
          ExprNodeDesc colDesc = childReduceSink.getConf().getValueCols().get(i);
          exprs.add(colDesc);
          outputs.add(internalName);
          outputRS.put(nm[0], nm[1], new ColumnInfo(internalName, valueInfo
              .getType(), nm[0], valueInfo.getIsVirtualCol(), valueInfo.isHiddenVirtualCol()));
          colExprMap.put(internalName, colDesc);
        }

        SelectDesc select = new SelectDesc(exprs, outputs, false);

        SelectOperator sel = (SelectOperator) putOpInsertMap(
            OperatorFactory.getAndMakeChild(select, new RowSchema(outputRS
            .getColumnInfos()), input), outputRS, pGraphContext);

        sel.setColumnExprMap(colExprMap);

        // Insert the select operator in between.
        if (childOp != null) {
          sel.setChildOperators(childOp);
          for (Operator<? extends Serializable> ch : childOp) {
            ch.replaceParent(oldParent, sel);
          }
        }
      }

      private Operator<? extends Serializable> putOpInsertMap(
          Operator<? extends Serializable> op, RowResolver rr, ParseContext pGraphContext) {
        OpParseContext ctx = new OpParseContext(rr);
        pGraphContext.getOpParseCtx().put(op, ctx);
        return op;
      }
    }
  }
}
