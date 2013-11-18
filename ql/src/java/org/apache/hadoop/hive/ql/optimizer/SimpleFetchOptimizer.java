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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
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
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.ContentSummaryInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.EstimatableStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBExpr;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * Tries to convert simple fetch query to single fetch task, which fetches rows directly
 * from location of table/partition.
 */
public class SimpleFetchOptimizer {

  // 1. no samping
  // 2. for partitioned table, all filters should be targeted to partition column
  // 3. SelectOperator should be select star
  static final int MINIMAL = 0;

  // single sourced select only (no CTAS or insert)
  // no join, no groupby, no distinct, no lateral view, no subq/analyze command
  static final int MORE = 1;

  // execute query in pseudo MR mode. converts top level TS to FetchTask
  static final int ALL = 2;

  private final Log LOG = LogFactory.getLog(SimpleFetchOptimizer.class.getName());

  public boolean transform(ParseContext pctx, int mode) throws Exception {
    Map<String, Operator<? extends OperatorDesc>> topOps = pctx.getTopOps();
    if (mode != ALL && topOps.size() != 1) {
      return false;
    }
    List<FetchData> datas = new ArrayList<FetchData>();
    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : topOps.entrySet()) {
      String alias = entry.getKey();
      Operator topOp = entry.getValue();
      if (!(topOp instanceof TableScanOperator)) {
        return false;
      }
      TableScanOperator tableScan = (TableScanOperator) topOp;
      FetchData data = checkTree(pctx, alias, tableScan, mode);
      if (data == null) {
        return false;
      }
      datas.add(data);
    }
    if (mode == ALL && !checkThreshold(datas, pctx)) {
      return false;
    }
    List<FetchTask> tasks = new ArrayList<FetchTask>();
    for (FetchData data : datas) {
      tasks.add(data.convertToTask(pctx, mode));
    }
    if (mode != ALL) {
      pctx.setFetchTask(tasks.get(0));
    } else {
      prepareReducers(topOps.values());
      pctx.getRootTasks().addAll(tasks);
    }
    topOps.clear();
    return true;
  }

  public List<FetchTask> transform(MapredWork mapredWork, Context ctx) throws Exception {
    if (!checkThreshold(mapredWork, ctx)) {
      return null;
    }
    List<FetchTask> fetchers = new ArrayList<FetchTask>();
    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry :
        mapredWork.getAliasToWork().entrySet()) {
      FetchWork fetchWork = getFetchForAlias(mapredWork, entry.getKey());
      FetchTask fetchTask = (FetchTask) TaskFactory.get(fetchWork, null);
      fetchWork.setSource(entry.getValue());
      fetchWork.setPseudoMR(true);
      fetchers.add(fetchTask);
    }
    Operator<?> reducer = mapredWork.getReducer();
    if (reducer != null) {
      List<Operator<? extends OperatorDesc>> terminals = getMapTerminals(mapredWork);
      for (Operator<?> terminal : terminals) {
        terminal.setChildOperators(
            new ArrayList<Operator<? extends OperatorDesc>>(Arrays.asList(reducer)));
      }
      reducer.setParentOperators(terminals);
    }
    prepareReducers(mapredWork.getAliasToWork().values());
    return fetchers;
  }

  public void untransform(MapredWork mapredWork) {
    for (Operator<?> operator : mapredWork.getAliasToWork().values()) {
      operator.clear();
    }
    Operator<?> reducer = mapredWork.getReducer();
    if (reducer != null) {
      for (Operator<?> parent : reducer.getParentOperators()) {
        parent.setChildOperators(null);
      }
      reducer.setParentOperators(null);
    }
  }

  private List<Operator<? extends OperatorDesc>> getMapTerminals(MapredWork mapredWork) {
    List<Operator<? extends OperatorDesc>> found =
        new ArrayList<Operator<? extends OperatorDesc>>();
    for (Operator<?> operator : mapredWork.getAliasToWork().values()) {
      getMapTerminals(operator, found);
    }
    return found;
  }

  private void getMapTerminals(Operator<?> operator, List<Operator<?>> found) {
    if (operator.getNumChild() == 0) {
      found.add(operator);
      return;
    }
    for (Operator<?> child : operator.getChildOperators()) {
      getMapTerminals(child, found);
    }
  }

  private FetchWork getFetchForAlias(MapredWork mapredWork, String alias) {
    String path = HiveFileFormatUtils.getPathForAlias(mapredWork.getPathToAliases(), alias);
    PartitionDesc partition = mapredWork.getPathToPartitionInfo().get(path);
    assert partition.getPartSpec() == null || partition.getPartSpec().isEmpty();
    FetchWork work = new FetchWork(path, partition.getTableDesc());
    return work;
  }

  private boolean checkThreshold(MapredWork mapredWork, Context ctx) throws Exception {
    long threshold = HiveConf.getLongVar(ctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONTHRESHOLD);
    if (threshold < 0) {
      return true;
    }
    ContentSummary summary = Utilities.getInputSummary(ctx, mapredWork, null);
    return summary.getLength() < threshold;
  }

  private boolean checkThreshold(List<FetchData> datas, ParseContext pctx) throws Exception {
    long threshold = HiveConf.getLongVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONTHRESHOLD);
    if (threshold < 0) {
      return true;
    }
    long remaining = threshold;
    for (FetchData data : datas) {
      remaining -= data.getLength(pctx, remaining);
      if (remaining < 0) {
        LOG.info("Threshold " + threshold + " exceeded for pseudoMR mode");
        return false;
      }
    }
    return true;
  }

  private FetchData checkTree(ParseContext pctx, String alias, TableScanOperator ts, int mode)
      throws HiveException {
    QB qb = pctx.getQB();
    boolean convertInsert = HiveConf.getBoolVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONINSERT);
    SplitSample splitSample = pctx.getNameToSplitSample().get(alias);
    if (mode == MINIMAL &&
        (splitSample != null || !qb.isSimpleSelectQuery() || qb.hasTableSample(alias))) {
      return null;
    }
    if (mode == MORE && !qb.isSimpleSelectQuery()) {
      return null;
    }
    boolean isQuery = qb.getIsQuery() && !qb.isCTAS();
    if (mode == ALL && !isQuery && !convertInsert) {
      return null;
    }

    String[] subqIDs = alias.split(":");
    for (int i = 0 ; i < subqIDs.length - 1; i++) {
      String[] subqID = subqIDs[i].split("-");
      if (subqID[0].equals("null")) {
        continue; // root alias
      }
      QBExpr qbexpr = qb.getSubqForAlias(subqID[0]);
      for (int j = 1; j < subqID.length; j++) {
        if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
          if (subqID[j].equals("subquery1")) {
            qbexpr = qbexpr.getQBExpr1();
          } else if (subqID[j].equals("subquery2")) {
            qbexpr = qbexpr.getQBExpr2();
          }
        }
      }
      qb = qbexpr.getQB();
    }
    String tableName = subqIDs[subqIDs.length - 1];
    Table table = qb.getMetaData().getAliasToTable().get(tableName);
    if (table == null) {
      return null;
    }
    ReadEntity parent = PlanUtils.getParentViewInfo(alias, pctx.getViewAliasToInput());
    if (!table.isPartitioned()) {
      return checkOperators(new FetchData(parent, table, ts, splitSample), ts, mode, false);
    }

    boolean bypassFilter = false;
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVEOPTPPD)) {
      ExprNodeDesc pruner = pctx.getOpToPartPruner().get(ts);
      bypassFilter = PartitionPruner.onlyContainsPartnCols(table, pruner);
    }
    if (mode != MINIMAL || bypassFilter) {
      PrunedPartitionList pruned = pctx.getPrunedPartitions(alias, ts);
      if (mode != MINIMAL || pruned.getUnknownPartns().isEmpty()) {
        bypassFilter &= pruned.getUnknownPartns().isEmpty();
        FetchData fetch = new FetchData(parent, table, ts, pruned, splitSample);
        return checkOperators(fetch, ts, mode, bypassFilter);
      }
    }
    return null;
  }

  // all we can handle is LimitOperator, FilterOperator SelectOperator and final FS
  private FetchData checkOperators(FetchData fetch, TableScanOperator ts, int mode,
      boolean bypassFilter) {
    if (mode == ALL) {
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
    if (op instanceof FileSinkOperator &&
        op.getChildOperators() == null || op.getChildOperators().isEmpty()) {
      fetch.fileSink = op;
      return fetch;
    }
    return null;
  }

  private class FetchData {

    private final ReadEntity parent;
    private final Table table;
    private final TableScanOperator scanOp;
    private final SplitSample splitSample;
    private final PrunedPartitionList partsList;
    private final HashSet<ReadEntity> inputs = new HashSet<ReadEntity>();

    // this is always non-null when query is converted to MORE more fetch
    private Operator<?> fileSink;

    private FetchData(ReadEntity parent, Table table, TableScanOperator scanOp,
        SplitSample splitSample) {
      this.parent = parent;
      this.table = table;
      this.scanOp = scanOp;
      this.partsList = null;
      this.splitSample = splitSample;
    }

    private FetchData(ReadEntity parent, Table table, TableScanOperator scanOp,
        PrunedPartitionList partsList, SplitSample splitSample) {
      this.parent = parent;
      this.table = table;
      this.scanOp = scanOp;
      this.partsList = partsList;
      this.splitSample = splitSample;
    }

      @SuppressWarnings("unchecked")
    private FetchTask convertToTask(ParseContext pctx, int mode) throws HiveException {
      int limit = pctx.getQB().getParseInfo().getOuterQueryLimit();
      FetchWork fetchWork = convertToWork();
      FetchTask fetchTask = (FetchTask) TaskFactory.get(fetchWork, pctx.getConf());
      fetchWork.setSink(completed(pctx, fetchWork, mode));
      fetchWork.setSource(scanOp);
      fetchWork.setLimit(limit);
      return fetchTask;
    }

    private FetchWork convertToWork() throws HiveException {
      inputs.clear();
      if (!table.isPartitioned()) {
        inputs.add(new ReadEntity(table, parent));
        String path = table.getPath().toString();
        FetchWork work = new FetchWork(path, Utilities.getTableDesc(table));
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        work.setSplitSample(splitSample);
        return work;
      }
      List<String> listP = new ArrayList<String>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partsList.getNotDeniedPartns()) {
        inputs.add(new ReadEntity(partition, parent));
        listP.add(partition.getPartitionPath().toString());
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
        if (!pctx.getQB().getIsQuery() || !searchSoleFileSink(pctx)) {
          return null;
        }
      }
      ListSinkOperator sink = new ListSinkOperator();
      sink.setConf(new ListSinkDesc(work.getSerializationNullFormat()));
      sink.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
      Operator<? extends OperatorDesc> parent = fileSink.getParentOperators().get(0);
      sink.getParentOperators().add(parent);
      parent.replaceChild(fileSink, sink);
      fileSink.setParentOperators(null);
      return sink;
    }

    private boolean searchSoleFileSink(ParseContext pctx) {
      Set<Operator<?>> found = new HashSet<Operator<?>>();
      for (Operator<?> operator : pctx.getTopOps().values()) {
        searchOperator(operator, FileSinkOperator.class, found);
      }
      if (found.size() != 1) {
        return false;
      }
      fileSink = found.iterator().next();
      return true;
    }

   private long getLength(ParseContext pctx, long remaining) throws Exception {
     long length = calculateLength(pctx, remaining);
     if (splitSample != null) {
       return (long) (length * splitSample.getPercent() / 100D);
     }
     return length;
   }

    private long calculateLength(ParseContext pctx, long remaining) throws Exception {
      JobConf jobConf = new JobConf(pctx.getConf());
      Utilities.setColumnNameList(jobConf, scanOp, true);
      Utilities.setColumnTypeList(jobConf, scanOp, true);

      HiveStorageHandler handler = table.getStorageHandler();
      if (handler instanceof EstimatableStorageHandler) {
        EstimatableStorageHandler estimator = (EstimatableStorageHandler) handler;
        TableDesc tableDesc = Utilities.getTableDesc(table);
        PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc);
        Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf);
        return estimator.estimate(jobConf, scanOp, remaining).getTotalLength();
      }
      if (!table.isPartitioned()) {
        Path path = table.getPath();
        return getLength(jobConf, path, table.getInputFormatClass());
      }
      long total = 0;
      for (Partition partition : partsList.getNotDeniedPartns()) {
        Path path = partition.getPartitionPath();
        total += getLength(jobConf, path, partition.getInputFormatClass());
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

  private void searchOperator(Operator<?> operator, Class<?> finding, Set<Operator<?>> found) {
    if (finding.isInstance(operator)) {
      found.add(operator);
    }
    if (operator.getChildOperators() != null) {
      for (Operator<?> child : operator.getChildOperators()) {
        searchOperator(child, finding, found);
      }
    }
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
