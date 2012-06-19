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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardTask;
import org.apache.hadoop.hive.ql.exec.HashReducer;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.ContentSummaryInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InMemoryProcessOptimizer {

  static final int SIMPLE = 0;
  static final int SIMPLE_AGGRESSIVE = 1;
  static final int FULL_AGGRESSIVE = 2;

  private final Log LOG = LogFactory.getLog(InMemoryProcessOptimizer.class.getName());

  public boolean transform(ParseContext pctx, int mode) throws SemanticException {
    Map<String, Operator<? extends Serializable>> topOps = pctx.getTopOps();
    try {
      if (!optimize(pctx, mode, topOps)) {
        return false;
      }
    } catch (Exception e) {
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (e instanceof SemanticException) {
        throw (SemanticException) e;
      }
      throw new SemanticException(e.getMessage(), e);
    }
    topOps.clear();
    return true;
  }

  private boolean optimize(ParseContext pctx, int mode, Map<String, Operator<? extends Serializable>> topOps) throws Exception {
    List<FetchData> fetchs = new ArrayList<FetchData>();
    for (Map.Entry<String, Operator<? extends Serializable>> entry : topOps.entrySet()) {
      String alias = entry.getKey();
      Operator topOp = entry.getValue();
      if (!(topOp instanceof TableScanOperator)) {
        return false;
      }
      FetchData fetch = checkTree(pctx, alias, (TableScanOperator) topOp, mode);
      if (fetch == null) {
        return false;
      }
      fetchs.add(fetch);
    }
    if (mode == FULL_AGGRESSIVE) {
      long limit = HiveConf.getLongVar(pctx.getConf(),
          HiveConf.ConfVars.HIVENOMRCONVERSIONTHRESHOLD);
      if (limit > 0) {
        for (FetchData fetch : fetchs) {
          limit -= fetch.getLength(pctx);
          if (limit < 0) {
            return false;
          }
        }
      }
    }
    List<Task<?>> tasks = new ArrayList<Task<?>>();
    for (FetchData fetch : fetchs) {
      tasks.add(convert(pctx, fetch, mode));
    }
    for (FetchData fetch : fetchs) {
      fetch.completed(pctx, mode);
    }
    if (mode != FULL_AGGRESSIVE) {
      pctx.setFetchTask((FetchTask) tasks.get(0));
    } else {
      prepareReducers(topOps.values());
      pctx.getRootTasks().addAll(tasks);
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  private Task<?> convert(ParseContext pctx, FetchData fetch, int mode) throws HiveException {
    int limit = pctx.getQB().getParseInfo().getOuterQueryLimit();
    FetchWork fetchWork = fetch.convertToWork(limit, fetch.scan);
    if (mode != FULL_AGGRESSIVE) {
      return TaskFactory.get(fetchWork, pctx.getConf());
    }
    Task<?> task = new ForwardTask(fetchWork);
    task.setId("Stage-" + TaskFactory.getAndIncrementId());
    return task;
  }

  private FetchData checkTree(ParseContext pctx, String alias,
      TableScanOperator ts, int mode) throws HiveException {
    SplitSample sample = pctx.getNameToSplitSample().get(alias);
    if (mode == SIMPLE && sample != null) {
      return null;
    }
    QB qb = pctx.getQB();
    if (mode == SIMPLE && qb.hasTableSample(alias)) {
      return null;
    }

    Table table = qb.getMetaData().getAliasToTable().get(alias);
    if (table == null) {
      return null;
    }
    Map<String, String> props = qb.getTabPropsForAlias(alias);
    if (!table.isPartitioned()) {
      return checkOperators(new FetchData(table, sample, props), ts, mode, false);
    }

    boolean bypassFilter = false;
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVEOPTPPD)) {
      ExprNodeDesc pruner = pctx.getOpToPartPruner().get(ts);
      bypassFilter = PartitionPruner.onlyContainsPartnCols(table, pruner);
    }
    if (mode != SIMPLE || bypassFilter) {
      PrunedPartitionList prunedPartitions = pctx.getPrunedPartitions(alias, ts);
      if (mode != SIMPLE || prunedPartitions.getUnknownPartns().isEmpty()) {
        bypassFilter &= prunedPartitions.getUnknownPartns().isEmpty();
        return checkOperators(new FetchData(prunedPartitions, sample, props), ts, mode, bypassFilter);
      }
    }
    return null;
  }

  private FetchData checkOperators(FetchData fetch, TableScanOperator ts, int mode,
      boolean bypassFilter) {
    if (mode == FULL_AGGRESSIVE) {
      fetch.needProcessor = true;
      fetch.scan = ts;
      return fetch;
    }
    if (mode == SIMPLE && (ts.getConf().getFilterExpr() != null ||
        (ts.getConf().getVirtualCols() != null && !ts.getConf().getVirtualCols().isEmpty()))) {
      return null;
    }
    if (ts.getChildOperators().size() != 1) {
      return null;
    }
    boolean needProcessor = ts.getConf().getFilterExpr() != null;
    Operator<?> op = ts.getChildOperators().get(0);
    for (; ; op = op.getChildOperators().get(0)) {
      if (!(op instanceof LimitOperator || op instanceof FilterOperator
          || op instanceof SelectOperator)) {
        break;
      }
      needProcessor |= op instanceof FilterOperator && !bypassFilter ||
          op instanceof SelectOperator && !((SelectOperator)op).getConf().isSelectStar();

      if (mode == SIMPLE && needProcessor) {
        return null;
      }
      if (op.getChildOperators() == null || op.getChildOperators().size() != 1) {
        return null;
      }
    }
    if (op instanceof FileSinkOperator &&
        op.getChildOperators() == null || op.getChildOperators().isEmpty()) {
      fetch.needProcessor = needProcessor;
      fetch.scan = ts;
      fetch.sink = (FileSinkOperator) op;
      return fetch;
    }
    return null;
  }

  private class FetchData {

    private Table table;
    private SplitSample splitSample;
    private List<Partition> partitions = new ArrayList<Partition>();
    private HashSet<ReadEntity> inputs = new HashSet<ReadEntity>();

    private TableScanOperator scan;
    private FileSinkOperator sink;
    private boolean needProcessor;
    private Map<String, String> props;

    private FetchData(Table table, SplitSample splitSample, Map<String, String> props) {
      this.table = table;
      this.splitSample = splitSample;
      this.props = props;
    }

    private FetchData(PrunedPartitionList partsList, SplitSample splitSample, Map<String, String> props) {
      partitions.addAll(partsList.getUnknownPartns());
      partitions.addAll(partsList.getConfirmedPartns());
      this.splitSample = splitSample;
      this.props = props;
    }

    private FetchWork convertToWork(int limit, Operator<?> processor) throws HiveException {
      FetchWork fetchWork = convertToWork(limit);
      if (needProcessor) {
        fetchWork.setProcessor(processor);
      }
      return fetchWork;
    }

    private FetchWork convertToWork(int limit) throws HiveException {
      if (table != null) {
        inputs.add(new ReadEntity(table));
        String path = table.getPath().toString();
        FetchWork work = new FetchWork(path, Utilities.getTableDesc(table, props), limit);
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        work.setSplitSample(splitSample);
        return work;
      }
      List<String> listP = new ArrayList<String>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partitions) {
        inputs.add(new ReadEntity(partition));
        listP.add(partition.getPartitionPath().toString());
        partP.add(Utilities.getPartitionDesc(partition, props));
      }
      FetchWork work = new FetchWork(listP, partP, limit);
      if (!work.getPartDesc().isEmpty()) {
        PartitionDesc part0 = work.getPartDesc().get(0);
        PlanUtils.configureInputJobPropertiesForStorageHandler(part0.getTableDesc());
        work.setSplitSample(splitSample);
      }
      return work;
    }

    private void completed(ParseContext pctx, int mode) {
      pctx.getSemanticInputs().addAll(inputs);
      if (mode != FULL_AGGRESSIVE) {
        sink.getParentOperators().get(0).setChildOperators(null);
        sink.setParentOperators(null);
      }
    }

    private long getLength(ParseContext pctx) throws Exception {
      long length = getLength(pctx.getContext());
      if (splitSample != null) {
        return (long) (length * splitSample.getPercent() / 100D);
      }
      return length;
    }

    private long getLength(Context context) throws Exception {
      JobConf conf = new JobConf(context.getConf());
      if (table != null) {
        Path path = table.getPath();
        return getLength(conf, path, table.getInputFormatClass());
      }
      long total = 0;
      for (Partition partition : partitions) {
        Path path = partition.getPartitionPath();
        total += getLength(conf, path, partition.getInputFormatClass());
      }
      return total;
    }

    private long getLength(JobConf conf, Path path, Class<? extends InputFormat> clazz) throws IOException {
      ContentSummary summary;
      if (ContentSummaryInputFormat.class.isAssignableFrom(clazz)) {
        InputFormat input = HiveInputFormat.getInputFormatFromCache(clazz, conf);
        summary = ((ContentSummaryInputFormat)input).getContentSummary(path, conf);
      } else {
        summary = path.getFileSystem(conf).getContentSummary(path);
      }
      return summary.getLength();
    }

  }

  private void prepareReducers(Collection<Operator<? extends Serializable>> operators) {
    Set<Operator<? extends Serializable>> targets =
        new HashSet<Operator<? extends Serializable>>();
    for (Operator<?> operator : operators) {
      targets.addAll(createReducer(operator, operator));
    }
    if (!targets.isEmpty()) {
      prepareReducers(targets);
    }
  }

  private Set<Operator<?>> createReducer(Operator<?> start, Operator<?> current) {
    if (current == null || current.getChildOperators() == null) {
      return Collections.emptySet();
    }
    Set<Operator<?>> next = new HashSet<Operator<?>>();
    if (current instanceof ReduceSinkOperator && current.getChildOperators() != null) {
      ReduceSinkOperator rs = (ReduceSinkOperator) current;
      for (Operator<?> child : rs.getChildOperators()) {
        if (child.getHashReducer() == null) {
          child.setHashReducer(new HashReducer(child, rs));
        }
        child.getHashReducer().setValueDesc(rs);
        start.setOutputCollector(child.getHashReducer());
        next.add(child);
      }
      return next;
    }
    for (Operator<?> child : current.getChildOperators()) {
      next.addAll(createReducer(start, child));
    }
    return next;
  }
}
