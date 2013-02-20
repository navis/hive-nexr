/**
 * Copyright 2010 The Apache Software Foundation
 *
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalPlanResolver;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SamplingContext;
import org.apache.hadoop.hive.ql.plan.TableDesc;

public class SamplingOptimizer implements Transform, PhysicalPlanResolver {

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    String RS = ReduceSinkOperator.getOperatorName() + "%";

    // generate pruned column list for all relevant operators
    SamplingProcCtx cppCtx = new SamplingProcCtx(pctx);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", RS + ".*" + RS), new FileSinkReducerProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, cppCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private class SamplingProcCtx implements NodeProcessorCtx {

    private ParseContext pctx;
    private List<ReduceSinkOperator> rejectedRSList;

    public SamplingProcCtx(ParseContext pctx) {
      this.pctx = pctx;
      this.rejectedRSList = new ArrayList<ReduceSinkOperator>();
    }

    public boolean contains(ReduceSinkOperator rsOp) {
      return rejectedRSList.contains(rsOp);
    }

    public void addRejectedReduceSinkOperator(ReduceSinkOperator rsOp) {
      if (!rejectedRSList.contains(rsOp)) {
        rejectedRSList.add(rsOp);
      }
    }
  }

  private static class FileSinkReducerProc implements NodeProcessor {

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SamplingProcCtx ctx = (SamplingProcCtx) procCtx;
      ReduceSinkOperator child = (ReduceSinkOperator) nd;
      if (ctx.contains(child)) {
        return null;
      }
      ReduceSinkDesc rsDesc = child.getConf();
      if (rsDesc.getNumReducers() != 1 || !rsDesc.getPartitionCols().isEmpty()) {
        ctx.addRejectedReduceSinkOperator(child);
        return null;
      }
      ReduceSinkOperator parent = OperatorUtils.findSingleParent(child, ReduceSinkOperator.class);
      if (parent == null) {
        return null;
      }

      int sampleNum = ctx.pctx.getConf().getIntVar(HiveConf.ConfVars.HIVESAMPLINGNUMBERFORORDERBY);

      List<ExprNodeDesc> exprs = rsDesc.getKeyCols();
      String order = rsDesc.getOrder();
      List<ExprNodeDesc> converted = new ArrayList<ExprNodeDesc>();
      List<FieldSchema> fields = new ArrayList<FieldSchema>();
      for (int i = 0; i < exprs.size(); i++) {
        ExprNodeDesc backtrack = ExprNodeDescUtils.backtrack(exprs.get(i), child, null);
        fields.add(MetaStoreUtils.getFieldSchemaFromTypeInfo(
            "_sampled_" + i, backtrack.getTypeInfo()));
        converted.add(backtrack);
      }
      TableDesc desc = PlanUtils.getReduceKeyTableDesc(fields, order);
      SamplingContext context = new SamplingContext();
      context.setSamplingKeys(converted);
      context.setSamplingNum(sampleNum);
      context.setTableInfo(desc);

      rsDesc.setNumReducers(-1);
      rsDesc.setSamplingContext(context);
      return null;
    }
  }

  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    for (Task<?> task : pctx.getRootTasks()) {
      if (!(task instanceof MapRedTask) || !((MapRedTask)task).getWork().isFinalMapRed()) {
        continue; // this could be replaced by bucketing on RS + bucketed fetcher for next MR
      }
      MapredWork mapreWork = ((MapRedTask) task).getWork();
      if (mapreWork.getNumReduceTasks() != 1 || mapreWork.getAliasToWork().size() != 1 ||
          mapreWork.getSamplingType() > 0 || mapreWork.getReducer() == null) {
        continue;
      }
      Operator<?> operator = mapreWork.getAliasToWork().values().iterator().next();
      if (!(operator instanceof TableScanOperator)) {
        continue;
      }
      ReduceSinkOperator child = OperatorUtils.findSingleChild(
          OperatorUtils.getSingleChild(operator), ReduceSinkOperator.class);
      if (child == null ||
          child.getConf().getNumReducers() != 1 || !child.getConf().getPartitionCols().isEmpty()) {
        continue;
      }
      if (OperatorUtils.findSingleChild(mapreWork.getReducer(), FileSinkOperator.class) == null) {
        continue;
      }
      child.getConf().setNumReducers(-1);
      mapreWork.setNumReduceTasks(-1);
      mapreWork.setSamplingType(MapredWork.DO_SAMPLING);
    }
    return pctx;
  }
}
