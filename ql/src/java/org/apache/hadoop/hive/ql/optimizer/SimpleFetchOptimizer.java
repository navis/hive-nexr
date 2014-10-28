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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.HashReducer;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
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
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
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
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Tries to convert simple fetch query to single fetch task, which fetches rows directly
 * from location of table/partition.
 */
public class SimpleFetchOptimizer {

  // 1. no sampling
  // 2. for partitioned table, all filters should be targeted to partition column
  // 3. SelectOperator should be select star
  static final int MINIMAL = 0;

  // single sourced select only (no CTAS or insert)
  // no join, no groupby, no distinct, no lateral view, no subq
  // not analyze command
  static final int MORE = 1;

  // all
  static final int ALL = 2;

  private final Log LOG = LogFactory.getLog(SimpleFetchOptimizer.class.getName());

  public boolean transform(ParseContext pctx, int mode) throws SemanticException {
    try {
      return _transform(pctx, mode);
    } catch (Exception e) {
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (e instanceof SemanticException) {
        throw (SemanticException) e;
      }
      throw new SemanticException(e.getMessage(), e);
    }
  }

  private boolean _transform(ParseContext pctx, int mode) throws Exception {
    Map<String, Operator<? extends OperatorDesc>> topOps = pctx.getTopOps();
    if (mode != ALL && topOps.size() != 1) {
      return false;
    }
    List<FetchData> fetchData = new ArrayList<FetchData>();
    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : topOps.entrySet()) {
      String alias = entry.getKey();
      Operator topOp = entry.getValue();
      if (!(topOp instanceof TableScanOperator)) {
        return false;
      }
      FetchData fetch = checkTree(pctx, alias, (TableScanOperator) topOp, mode);
      if (fetch == null) {
        return false;
      }
      fetchData.add(fetch);
    }
    long remaining = HiveConf.getLongVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONTHRESHOLD);
    if (remaining > 0) {
      for (FetchData data : fetchData) {
        remaining -= data.getInputLength(pctx, remaining);
        if (remaining < 0) {
          LOG.info("Threshold " + remaining + " exceeded for pseudoMR mode");
          return false;
        }
      }
    }
    int outerLimit = pctx.getQB().getParseInfo().getOuterQueryLimit();
    List<Task<?>> tasks = new ArrayList<Task<?>>();
    for (FetchData data : fetchData) {
      tasks.add(convertToTask(pctx, data, mode, outerLimit));
    }
    if (mode != ALL) {
      assert tasks.size() == 1;
      pctx.setFetchTask((FetchTask) tasks.get(0));
    } else {
      prepareReducers(topOps.values());
      pctx.getRootTasks().addAll(tasks);
    }
    topOps.clear();
    return true;
  }

  @SuppressWarnings("unchecked")
  private Task<?> convertToTask(ParseContext pctx, FetchData fetch, int mode, int outerLimit)
      throws HiveException {
    FetchWork fetchWork = fetch.convertToWork();
    fetchWork.setSink(fetch.completed(pctx, fetchWork, mode));
    fetchWork.setLimit(outerLimit);
    return TaskFactory.get(fetchWork, pctx.getConf());
  }


  // all we can handle is LimitOperator, FilterOperator SelectOperator and final FS
  private FetchData checkTree(ParseContext pctx, String alias, TableScanOperator ts, int mode)
      throws HiveException {
    QB qb = pctx.getQB();
    if (qb.isAnalyze() || qb.isCTAS()) {
      return null;
    }
    SplitSample splitSample = pctx.getNameToSplitSample().get(alias);
    if (mode == MINIMAL &&
        (splitSample != null || !qb.isSimpleSelectQuery() || qb.hasTableSample(alias))) {
      return null;
    }
    if (mode == MORE && !qb.isSimpleSelectQuery()) {
      return null;
    }

    Table table = qb.getMetaData().getAliasToTable().get(alias);
    if (table == null) {
      return null;
    }
    ReadEntity parent = PlanUtils.getParentViewInfo(alias, pctx.getViewAliasToInput());
    if (!table.isPartitioned()) {
      return checkOperators(new FetchData(parent, table, splitSample), ts, mode, false);
    }

    boolean bypassFilter = false;
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVEOPTPPD)) {
      ExprNodeDesc pruner = pctx.getOpToPartPruner().get(ts);
      bypassFilter = PartitionPruner.onlyContainsPartnCols(table, pruner);
    }
    if (mode != MINIMAL || bypassFilter) {
      PrunedPartitionList pruned = pctx.getPrunedPartitions(alias, ts);
      if (mode != MINIMAL || !pruned.hasUnknownPartitions()) {
        bypassFilter &= !pruned.hasUnknownPartitions();
        return checkOperators(new FetchData(parent, table, pruned, splitSample), ts, mode, bypassFilter);
      }
    }
    return null;
  }

  private FetchData checkOperators(FetchData fetch, TableScanOperator ts, int mode,
      boolean bypassFilter) {
    if (mode == ALL) {
      fetch.scanOp = ts;
      return fetch;
    }
    if (mode == MINIMAL && (ts.getConf().getFilterExpr() != null ||
        (ts.getConf().getVirtualCols() != null && !ts.getConf().getVirtualCols().isEmpty()))) {
      return null;
    }
    if (ts.getChildOperators().size() != 1) {
      return null;
    }
    boolean needProcessor = ts.getConf().getFilterExpr() != null;
    Operator<?> op = ts.getChildOperators().get(0);
    for (; ; op = op.getChildOperators().get(0)) {
      if (mode == MORE) {
        if (!(op instanceof LimitOperator || op instanceof FilterOperator
            || op instanceof SelectOperator)) {
          break;
        }
      } else if (!(op instanceof LimitOperator || (op instanceof FilterOperator && bypassFilter)
          || (op instanceof SelectOperator && ((SelectOperator) op).getConf().isSelectStar()))) {
        break;
      }
      if (op.getChildOperators() == null || op.getChildOperators().size() != 1) {
        return null;
      }
    }
    if (op instanceof FileSinkOperator && op.getNumChild() == 0) {
      fetch.scanOp = ts;
      fetch.fileSink = op;
      return fetch;
    }
    return null;
  }

  private class FetchData {

    private final ReadEntity parent;
    private final Table table;
    private final SplitSample splitSample;
    private final PrunedPartitionList partsList;
    private final HashSet<ReadEntity> inputs = new HashSet<ReadEntity>();

    // source table scan
    private TableScanOperator scanOp;

    // this is always non-null when query is converted to MORE more fetch
    private Operator<?> fileSink;

    private FetchData(ReadEntity parent, Table table, SplitSample splitSample) {
      this.parent = parent;
      this.table = table;
      this.partsList = null;
      this.splitSample = splitSample;
    }

    private FetchData(ReadEntity parent, Table table, PrunedPartitionList partsList,
        SplitSample splitSample) {
      this.parent = parent;
      this.table = table;
      this.partsList = partsList;
      this.splitSample = splitSample;
    }

    private FetchWork convertToWork() throws HiveException {
      inputs.clear();
      if (!table.isPartitioned()) {
        inputs.add(new ReadEntity(table, parent));
        FetchWork work = new FetchWork(table.getPath(), Utilities.getTableDesc(table));
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        work.setSplitSample(splitSample);
        work.setSource(scanOp);
        return work;
      }
      List<Path> listP = new ArrayList<Path>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partsList.getNotDeniedPartns()) {
        inputs.add(new ReadEntity(partition, parent));
        listP.add(partition.getDataLocation());
        partP.add(Utilities.getPartitionDesc(partition));
      }
      Table sourceTable = partsList.getSourceTable();
      inputs.add(new ReadEntity(sourceTable, parent));
      TableDesc table = Utilities.getTableDesc(sourceTable);
      FetchWork work = new FetchWork(listP, partP, table);
      if (!work.getPartDesc().isEmpty()) {
        PartitionDesc part0 = work.getPartDesc().get(0);
        PlanUtils.configureInputJobPropertiesForStorageHandler(part0.getTableDesc());
        work.setSplitSample(splitSample);
      }
      work.setSource(scanOp);
      return work;
    }

    // this optimizer is for replacing FS to temp+fetching from temp with
    // single direct fetching, which means FS is not needed any more when conversion completed.
    // rows forwarded will be received by ListSinkOperator, which is replacing FS
    private ListSinkOperator completed(ParseContext pctx, FetchWork work, int mode) {
      for (ReadEntity input : inputs) {
        PlanUtils.addInput(pctx.getSemanticInputs(), input);
      }
      if (mode == ALL) {
        work.setPseudoMR(true);
        return null;
      }
      return replaceFSwithLS(fileSink, work.getSerializationNullFormat());
    }

    private long getInputLength(ParseContext pctx, long remaining) throws Exception {
      if (splitSample != null && splitSample.getTotalLength() != null) {
        return splitSample.getTotalLength();
      }
      long length = calculateLength(pctx, remaining);
      if (splitSample != null) {
        return splitSample.getTargetSize(length);
      }
      return length;
    }

    private long calculateLength(ParseContext pctx, long remaining) throws Exception {
      JobConf jobConf = new JobConf(pctx.getConf());
      Utilities.setColumnNameList(jobConf, scanOp, true);
      Utilities.setColumnTypeList(jobConf, scanOp, true);
      HiveStorageHandler handler = table.getStorageHandler();
      if (handler instanceof InputEstimator) {
        InputEstimator estimator = (InputEstimator) handler;
        TableDesc tableDesc = Utilities.getTableDesc(table);
        PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc);
        Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf);
        return estimator.estimate(jobConf, scanOp, remaining).getTotalLength();
      }
      if (table.isNonNative()) {
        return 0; // nothing can be done
      }
      if (!table.isPartitioned()) {
        return getFileLength(jobConf, table.getPath(), table.getInputFormatClass());
      }
      long total = 0;
      for (Partition partition : partsList.getNotDeniedPartns()) {
        Path path = partition.getDataLocation();
        total += getFileLength(jobConf, path, partition.getInputFormatClass());
      }
      return total;
    }

    // from Utilities.getInputSummary()
    private long getFileLength(JobConf conf, Path path, Class<? extends InputFormat> clazz)
        throws IOException {
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

  public static ListSinkOperator replaceFSwithLS(Operator<?> fileSink, String nullFormat) {
    ListSinkOperator sink = new ListSinkOperator();
    sink.setConf(new ListSinkDesc(nullFormat));

    sink.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
    Operator<? extends OperatorDesc> parent = fileSink.getParentOperators().get(0);
    sink.getParentOperators().add(parent);
    parent.replaceChild(fileSink, sink);
    fileSink.setParentOperators(null);
    return sink;
  }

  private void prepareReducers(Collection<Operator<? extends OperatorDesc>> operators) {
    Set<Operator<? extends OperatorDesc>> targets =
        new HashSet<Operator<? extends OperatorDesc>>();
    for (Operator<?> operator : operators) {
      targets.addAll(createReducer(operator, operator));
    }
    if (!targets.isEmpty()) {
      prepareReducers(targets);
    }
  }

  // set  hash reducer from start to child of RS
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
