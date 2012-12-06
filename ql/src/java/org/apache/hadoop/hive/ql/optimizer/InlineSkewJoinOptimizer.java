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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.ql.exec.ByteWritable;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SkewContext;
import org.apache.hadoop.hive.ql.udf.generic.NumericHistogram;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class InlineSkewJoinOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(InlineSkewJoinOptimizer.class);

  public ParseContext transform(ParseContext pctx) throws SemanticException {
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "JOIN%"), new InlineSkewJoinProcessor());
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, new SkewOptContext(pctx));
    GraphWalker ogw = new DefaultGraphWalker(disp);

    List<Node> topNodes = new ArrayList<Node>(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private static class SkewOptContext implements NodeProcessorCtx {
    private ParseContext parseContext;
    public SkewOptContext(ParseContext pctx) {
      this.parseContext = pctx;
    }
  }

  private static class InlineSkewJoinProcessor implements NodeProcessor {

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ParseContext pctx = ((SkewOptContext)procCtx).parseContext;
      JoinOperator join = (JoinOperator) nd;
      QBJoinTree jointTree = pctx.getJoinContext().get(join);
      if (jointTree.getSkewExprs() != null && !jointTree.getSkewExprs().isEmpty()) {
        return false;   // explicit skew context
      }

      Operator[] parents = (Operator[]) join.getParentOperators().toArray();

      // find driver pos, use biggest one if possible
      int driver = parents.length - 1;
      List<String> streams = jointTree.getStreamAliases();
      if  (streams != null && !streams.isEmpty()) {
        driver = JoinReorder.getBiggestPos(join, streams);
      }

      ReduceSinkOperator rs = (ReduceSinkOperator) parents[driver];
      TableScanOperator ts = findSingleParent(rs, TableScanOperator.class);
      if (ts == null || pctx.getTopToTable().get(ts) == null) {
        return false;
      }
      Table table = pctx.getTopToTable().get(ts);
      List<ExprNodeDesc> sources = rs.getConf().getKeyCols();

      // revert key expressions of RS to columns of TS
      List<ExprNodeDesc> backtrack = ExprNodeDescUtils.backtrack(sources, rs, ts);

      KeyDistribution[] skewness = getSkewness(pctx, table, backtrack);
      if (skewness == null || skewness.length == 0) {
        return false;
      }

      ArrayList<Boolean>[] skewDrivers = new ArrayList[parents.length];
      ArrayList<Integer>[] skewClusters = new ArrayList[parents.length];
      ArrayList<ExprNodeDesc>[] skewKeysExprs = new ArrayList[parents.length];

      for (KeyDistribution skew : skewness) {
        if (!skew.expression.getTypeString().equals("boolean")) {
          LOG.info("Boolean type expression is needed. " +
              skew.expression.getExprString() + " has type of " + skew.expression.getTypeString());
          return false;
        }
        int skewPercent = (int) (skew.percent * 100);
        for (int pos = 0; pos < parents.length; pos++) {
          if (skewKeysExprs[pos] == null) {
            skewDrivers[pos] = new ArrayList<Boolean>();
            skewClusters[pos] = new ArrayList<Integer>();
            skewKeysExprs[pos] = new ArrayList<ExprNodeDesc>();
          }
          skewDrivers[pos].add(pos == driver);
          skewClusters[pos].add(skewPercent);
          if (pos == driver) {
            skewKeysExprs[pos].add(skew.expression);
            continue;
          }
          ReduceSinkOperator input = (ReduceSinkOperator)parents[pos];
          List<ExprNodeDesc> targets = input.getConf().getKeyCols();
          ExprNodeDesc skewKey = ExprNodeDescUtils.replace(skew.expression, sources, targets);
          if (skewKey == null) {
            LOG.info("Failed to find join condition for " + skewKey);
            return false;
          }
          skewKeysExprs[pos].add(skewKey);
        }
      }

      // hand over skew context to all RSs for the join
      for (int pos = 0; pos < parents.length; pos++) {
        SkewContext context = new SkewContext();
        context.setSkewKeys(skewKeysExprs[pos]);
        context.setSkewDrivers(skewDrivers[pos]);
        context.setSkewClusters(skewClusters[pos]);
        ((ReduceSinkOperator) parents[pos]).getConf().setSkewContext(context);
      }
      return true;
    }

    private KeyDistribution[] getSkewness(ParseContext pctx, Table table,
        List<ExprNodeDesc> sources) throws SemanticException {
      try {
        return getSkewness(Hive.get(pctx.getConf()), table, sources);
      } catch (Exception e) {
        LOG.info("Failed to access column statistics for skewness", e);
      }
      return null;
    }

    private List<String> getPrimitiveColumns(List<ExprNodeDesc> sources) {
      Set<String> columns = new HashSet<String>();
      for (ExprNodeDesc source : sources) {
        if (isPrimitiveColumn(source)) {
          columns.add(((ExprNodeColumnDesc) source).getColumn());
        }
      }
      return columns.isEmpty() ? Collections.<String>emptyList() : new ArrayList<String>(columns);
    }

    private boolean isPrimitiveColumn(ExprNodeDesc source) {
      return source instanceof ExprNodeColumnDesc && isPrimitiveNumeric(source.getTypeInfo());
    }

    private KeyDistribution[] getSkewness(Hive hive, Table table, List<ExprNodeDesc> sources)
        throws Exception {

      List<String> columns = getPrimitiveColumns(sources);
      if (columns.isEmpty()) {
        return new KeyDistribution[0];
      }
      List<ColumnStatisticsObj> columnStats =
          hive.getTableColumnStatistics(table.getDbName(), table.getTableName(), columns);

      List<KeyDistribution> result = new ArrayList<KeyDistribution>();
      for (ExprNodeDesc source : sources) {
        if (!isPrimitiveColumn(source)) {
          continue;
        }
        PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) source.getTypeInfo();
        String column = ((ExprNodeColumnDesc)source).getColumn();

        ColumnStatisticsObj stats = columnStats.get(columns.indexOf(column));

          NumericHistogram histogram = getNumericHistogram(stats.getStatsData());
        if (histogram == null) {
          continue;
        }
        double[][] published = histogram.publish();
        if (published.length < 10) {
          continue;
        }
        long countSum = 0;
        for (int i = 0; i < published.length; i++) {
          countSum += (long) published[i][1];
        }
        for (int i = 0; i < published.length; i++) {
          if (published[i][1] * published.length > (countSum << 1)) {
            float percent = (float) (published[i][1] / countSum);
            ExprNodeDesc expr = null;
            if (i > 0) {
              List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
              children.add(source.clone());
              children.add(cast(published[i - 1][0], ptype));
              expr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                  FunctionRegistry.getFunctionInfo(">").getGenericUDF(), children);
              percent += published[i - 1][1] / countSum / 2;
            }
            if (i < published.length - 1) {
              List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
              children.add(source.clone());
              children.add(cast(published[i + 1][0], ptype));
              ExprNodeDesc expr2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                  FunctionRegistry.getFunctionInfo("<").getGenericUDF(), children);
              percent += published[i + 1][1] / countSum / 2;
              expr = expr == null ? expr2 : ExprNodeDescUtils.mergePredicates(expr, expr2);
            }
            result.add(new KeyDistribution(expr, percent));
          }
        }
      }
      return result.toArray(new KeyDistribution[result.size()]);
    }

    // TODO get histogram from column stats
    private NumericHistogram getNumericHistogram(ColumnStatisticsData data) {
      return null;
    }
  }

  private static boolean isPrimitiveNumeric(TypeInfo type) {
    if (type.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      return false;
    }
    switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return true;
    }
    return false;
  }

  private static ExprNodeConstantDesc cast(double value, PrimitiveTypeInfo ptype) {
    switch (ptype.getPrimitiveCategory()) {
      case BYTE:
        return new ExprNodeConstantDesc(ptype, new ByteWritable((byte)value));
      case SHORT:
        return new ExprNodeConstantDesc(ptype, new ShortWritable((short)value));
      case INT:
        return new ExprNodeConstantDesc(ptype, new IntWritable((int)value));
      case LONG:
        return new ExprNodeConstantDesc(ptype, new LongWritable((long)value));
      case FLOAT:
        return new ExprNodeConstantDesc(ptype, new FloatWritable((float)value));
      case DOUBLE:
        return new ExprNodeConstantDesc(ptype, new DoubleWritable(value));
    }
    throw new IllegalStateException("Not supported category " + ptype.getPrimitiveCategory());
  }

  private static <T extends Operator> T findSingleParent(Operator<?> start, Class<T> target) {
    if (start.getParentOperators() == null || start.getParentOperators().size() != 1) {
      return null;
    }
    Operator<? extends OperatorDesc> parent = start.getParentOperators().get(0);
    if (parent.getClass() == target) {
      return (T) parent;
    }
    return findSingleParent(parent, target);
  }

  public static class KeyDistribution {
    private ExprNodeDesc expression;
    private float percent;
    public KeyDistribution(ExprNodeDesc expression, float percent) {
      this.expression = expression;
      this.percent = percent;
    }
  }
}
