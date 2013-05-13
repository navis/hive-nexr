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
import org.apache.hadoop.hive.ql.plan.ListSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  // no join, no groupby, no distinct, no lateral view, no subq
  // not analyze command
  static final int MORE = 1;

  // convert TSs to pseudo MR tasks
  static final int ALL = 2;

  private final Log LOG = LogFactory.getLog(SimpleFetchOptimizer.class.getName());

  public boolean transform(ParseContext pctx, int mode) throws SemanticException {
    Map<String, Operator<? extends OperatorDesc>> topOps = pctx.getTopOps();
    if (mode != ALL && topOps.size() != 1) {
      return false;
    }
    List<Task<?>> tasks = new ArrayList<Task<?>>();
    for (Map.Entry<String, Operator<? extends OperatorDesc>> entry : topOps.entrySet()) {
      String alias = entry.getKey();
      Operator topOp = entry.getValue();
      if (!(topOp instanceof TableScanOperator)) {
        return false;
      }
      try {
        Task<?> task = optimize(pctx, alias, (TableScanOperator) topOp, mode);
        if (task == null) {
          return false;
        }
        tasks.add(task);
      } catch (HiveException e) {
        // Has to use full name to make sure it does not conflict with
        // org.apache.commons.lang.StringUtils
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        if (e instanceof SemanticException) {
          throw (SemanticException) e;
        }
        throw new SemanticException(e.getMessage(), e);
      }
    }
    if (mode != ALL) {
      pctx.setFetchTask((FetchTask) tasks.get(0));
    } else {
      prepareReducers(topOps.values());
      pctx.getRootTasks().addAll(tasks);
    }
    topOps.clear();
    return true;
  }

  // returns non-null FetchTask instance when succeeded
  private Task<?> optimize(ParseContext pctx, String alias, TableScanOperator source, int mode)
      throws HiveException {
    FetchData fetch = checkTree(pctx, alias, source, mode);
    if (fetch != null) {
      int limit = pctx.getQB().getParseInfo().getOuterQueryLimit();
      FetchWork fetchWork = fetch.convertToWork();

      @SuppressWarnings("unchecked")
      FetchTask fetchTask = (FetchTask) TaskFactory.get(fetchWork, pctx.getConf());
      fetchWork.setSink(fetch.completed(pctx, fetchWork, mode));
      fetchWork.setSource(source);
      fetchWork.setLimit(limit);
      return fetchTask;
    }
    return null;
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

    Table table = qb.getMetaData().getAliasToTable().get(alias);
    if (table == null) {
      return null;
    }
    if (!table.isPartitioned()) {
      return checkOperators(new FetchData(table, splitSample), ts, mode, false);
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
        return checkOperators(new FetchData(pruned, splitSample), ts, mode, bypassFilter);
      }
    }
    return null;
  }

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

    private final Table table;
    private final SplitSample splitSample;
    private final PrunedPartitionList partsList;
    private final HashSet<ReadEntity> inputs = new HashSet<ReadEntity>();

    // this is always non-null when query is converted to MORE more fetch
    private Operator<?> fileSink;

    private FetchData(Table table, SplitSample splitSample) {
      this.table = table;
      this.partsList = null;
      this.splitSample = splitSample;
    }

    private FetchData(PrunedPartitionList partsList, SplitSample splitSample) {
      this.table = null;
      this.partsList = partsList;
      this.splitSample = splitSample;
    }

    private FetchWork convertToWork() throws HiveException {
      inputs.clear();
      if (table != null) {
        inputs.add(new ReadEntity(table));
        String path = table.getPath().toString();
        FetchWork work = new FetchWork(path, Utilities.getTableDesc(table));
        PlanUtils.configureInputJobPropertiesForStorageHandler(work.getTblDesc());
        work.setSplitSample(splitSample);
        return work;
      }
      List<String> listP = new ArrayList<String>();
      List<PartitionDesc> partP = new ArrayList<PartitionDesc>();

      for (Partition partition : partsList.getNotDeniedPartns()) {
        inputs.add(new ReadEntity(partition));
        listP.add(partition.getPartitionPath().toString());
        partP.add(Utilities.getPartitionDesc(partition));
      }
      Table sourceTable = partsList.getSourceTable();
      inputs.add(new ReadEntity(sourceTable));
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
      pctx.getSemanticInputs().addAll(inputs);
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
