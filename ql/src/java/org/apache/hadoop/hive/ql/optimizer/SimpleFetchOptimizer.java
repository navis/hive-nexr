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
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
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
import org.apache.hadoop.hive.ql.parse.QBExpr;
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

  // convert TSs to pseudo MR tasks
  static final int ALL = 2;

  private final Log LOG = LogFactory.getLog(SimpleFetchOptimizer.class.getName());

  public boolean transform(ParseContext pctx, int mode) throws SemanticException {
    if (pctx.getQB().isAnalyze()) {
      return false;
    }
    FileSinkOperator fs = searchSoleFileSink(pctx);
    if (fs == null) {
      return false;
    }
    try {
      return _transform(pctx, fs, mode);
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

  private FileSinkOperator searchSoleFileSink(ParseContext pctx) {
    Collection<Operator<?>> topOps = pctx.getTopOps().values();
    Set<FileSinkOperator> found = OperatorUtils.findOperators(topOps, FileSinkOperator.class);
    if (found.size() != 1) {
      return null;
    }
    return found.iterator().next();
  }

  private boolean _transform(ParseContext pctx, FileSinkOperator fileSink, int mode) throws Exception {
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
    long threshold = HiveConf.getLongVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONTHRESHOLD);
    if (threshold > 0) {
      long remaining = threshold;
      for (FetchData data : fetchData) {
        remaining -= data.getInputLength(pctx, remaining);
        if (remaining < 0) {
          LOG.info("Input threshold for pseudoMR mode " + threshold + " was exceeded");
          return false;
        }
      }
    }

    int outerLimit = pctx.getQB().getParseInfo().getOuterQueryLimit();
    ListSinkOperator listSink = convertToListSink(pctx, fileSink, mode);

    List<FetchTask> tasks = new ArrayList<FetchTask>();
    for (FetchData data : fetchData) {
      FetchWork fetchWork = data.convertToWork();
      for (ReadEntity input : data.inputs) {
        PlanUtils.addInput(pctx.getSemanticInputs(), input);
      }
      fetchWork.setPseudoMR(mode == ALL);
      fetchWork.setLimit(outerLimit);
      fetchWork.setSink(listSink);
      tasks.add((FetchTask) TaskFactory.get(fetchWork, pctx.getConf()));
    }
    if (mode != ALL) {
      assert tasks.size() == 1;
      pctx.setFetchTask(tasks.get(0));
    } else {
      prepareReducers(topOps.values());
      pctx.getRootTasks().addAll(tasks);
    }
    topOps.clear();
    return true;
  }

  private ListSinkOperator convertToListSink(ParseContext pctx, FileSinkOperator sink, int mode) {
    if (mode == ALL && (!pctx.getQB().getIsQuery() || !HiveConf.getBoolVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONLISTFETCH))) {
      return null;
    }
    String nullFormat = HiveConf.getVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONNULLFORMAT);
    return replaceFSwithLS(sink, nullFormat);
  }

  // all we can handle is LimitOperator, FilterOperator SelectOperator and final FS
  private FetchData checkTree(ParseContext pctx, String alias, TableScanOperator ts, int mode)
      throws HiveException {
    QB qb = pctx.getQB();
    SplitSample splitSample = pctx.getNameToSplitSample().get(alias);
    if (mode == MINIMAL &&
        (splitSample != null || !qb.isSimpleSelectQuery() || qb.hasTableSample(alias))) {
      return null;
    }
    if (mode == MORE && !qb.isSimpleSelectQuery()) {
      return null;
    }
    boolean convertInsert = HiveConf.getBoolVar(pctx.getConf(),
        HiveConf.ConfVars.HIVEFETCHTASKCONVERSIONINSERT);
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
      return checkOperators(new FetchData(parent, table, ts, null, splitSample), mode, false);
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
        return checkOperators(
            new FetchData(parent, table, ts, pruned, splitSample), mode, bypassFilter);
      }
    }
    return null;
  }

  private FetchData checkOperators(FetchData fetch, int mode, boolean bypassFilter) {
    if (mode == ALL) {
      return fetch;
    }
    TableScanOperator ts = fetch.scanOp;
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

    private FetchData(ReadEntity parent, Table table, TableScanOperator scanOp,
        PrunedPartitionList partsList, SplitSample splitSample) {
      this.parent = parent;
      this.table = table;
      this.scanOp = scanOp;
      this.partsList = partsList;
      this.splitSample = splitSample;
    }

    private FetchWork convertToWork() throws HiveException {
      inputs.clear();
      if (!table.isPartitioned()) {
        inputs.add(new ReadEntity(table, scanOp, parent));
        FetchWork work = new FetchWork(table.getPath(), Utilities.getTableDesc(table));
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        work.setSplitSample(splitSample);
        work.setSource(scanOp);
        return work;
      }
      List<Path> listP = new ArrayList<Path>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partsList.getNotDeniedPartns()) {
        inputs.add(new ReadEntity(partition, scanOp, parent));
        listP.add(partition.getDataLocation());
        partP.add(Utilities.getPartitionDesc(partition));
      }
      Table sourceTable = partsList.getSourceTable();
      inputs.add(new ReadEntity(sourceTable, scanOp, parent));
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
