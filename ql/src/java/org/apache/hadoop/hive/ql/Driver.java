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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.AsynchronousCloseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.lockmgr.SharedLockManager;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationFactory;
import org.apache.hadoop.hive.ql.security.authorization.DelegatableAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

public class Driver implements CommandProcessor {

  static final private Log LOG = LogFactory.getLog(Driver.class.getName());
  static final private LogHelper console = new LogHelper(LOG);

  private static final Object compileMonitor = new Object();

  private static SharedLockManager sharedLockMgr;

  private int maxRows = 100;
  ByteStream.Output bos = new ByteStream.Output();

  private HiveConf conf;
  private DataInput resStream;
  private Context ctx;
  private DriverContext driverCxt;
  private QueryPlan plan;
  private Schema schema;
  private HiveLockManager hiveLockMgr;

  private String errorMessage;
  private String SQLState;

  private transient List<HiveDriverRunHook> driverRunHooks;
  private transient HiveDriverRunHookContext hookContext;

  // A limit on the number of threads that can be launched
  private int maxthreads;
  private int tryCount = Integer.MAX_VALUE;

  private boolean destroyed;

  // the reason that we set the lock manager for the cxt here is because each
  // query has its own ctx object. The hiveLockMgr is shared accross the
  // same instance of Driver, which can run multiple queries.
  private boolean checkLockManager() {
    if (sharedLockMgr != null || hiveLockMgr != null) {
      ctx.setHiveLockMgr(sharedLockMgr != null ? sharedLockMgr : hiveLockMgr);
      return true;
    }
    boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
    if (!supportConcurrency) {
      return false;
    }
    HiveLockManager lockManager;
    try {
      lockManager = createLockManager();
    } catch (SemanticException e) {
      errorMessage = "FAILED: Error in semantic analysis: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return false;
    }
    ctx.setHiveLockMgr(lockManager);

    if (lockManager instanceof SharedLockManager) {
      sharedLockMgr = (SharedLockManager) lockManager;
      Runtime.getRuntime().addShutdownHook(new LockShutdownHook());
    } else {
      hiveLockMgr = lockManager;
    }
    return lockManager != null;
  }

  private static class LockShutdownHook extends Thread {
    @Override
    public void run() {
      try {
        sharedLockMgr.close();
      } catch (Exception e) {
        LOG.error("Failed to close lock manager", e);
      }
      sharedLockMgr = null;
    }
  }

  private HiveLockManager createLockManager() throws SemanticException {
    String lockMgr = conf.getVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER);
    if (lockMgr == null || lockMgr.isEmpty()) {
      throw new SemanticException(ErrorMsg.LOCKMGR_NOT_SPECIFIED.getMsg());
    }
    HiveLockManager hiveLockMgr = null;
    try {
      hiveLockMgr = (HiveLockManager) ReflectionUtils.newInstance(conf.getClassByName(lockMgr),
          conf);
      hiveLockMgr.setContext(new HiveLockManagerCtx(conf));
    } catch (Exception e) {
      // set hiveLockMgr to null just in case this invalid manager got set to
      // next query's ctx.
      if (hiveLockMgr != null) {
        try {
          hiveLockMgr.close();
        } catch (Exception e1) {
          //nothing can do here
        }
      }
      throw new SemanticException(ErrorMsg.LOCKMGR_NOT_INITIALIZED.getMsg() + e.getMessage());
    }
    return hiveLockMgr;
  }

  public void init() {
    Operator.resetId();
  }

  /**
   * Return the status information about the Map-Reduce cluster
   */
  public ClusterStatus getClusterStatus() throws Exception {
    ClusterStatus cs;
    try {
      JobConf job = new JobConf(conf, ExecDriver.class);
      JobClient jc = new JobClient(job);
      cs = jc.getClusterStatus();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning cluster status: " + cs.toString());
    return cs;
  }


  public Schema getSchema() {
    return schema;
  }

  /**
   * Get a Schema with fields represented with native Hive types
   */
  public static Schema getSchema(BaseSemanticAnalyzer sem, HiveConf conf) {
    Schema schema = null;

    // If we have a plan, prefer its logical result schema if it's
    // available; otherwise, try digging out a fetch task; failing that,
    // give up.
    if (sem == null) {
      // can't get any info without a plan
    } else if (sem.getResultSchema() != null) {
      List<FieldSchema> lst = sem.getResultSchema();
      schema = new Schema(lst, null);
    } else if (sem.getFetchTask() != null) {
      FetchTask ft = sem.getFetchTask();
      TableDesc td = ft.getTblDesc();
      // partitioned tables don't have tableDesc set on the FetchTask. Instead
      // they have a list of PartitionDesc objects, each with a table desc.
      // Let's
      // try to fetch the desc for the first partition and use it's
      // deserializer.
      if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
        if (ft.getWork().getPartDesc().size() > 0) {
          td = ft.getWork().getPartDesc().get(0).getTableDesc();
        }
      }

      if (td == null) {
        LOG.info("No returning schema.");
      } else {
        String tableName = "result";
        List<FieldSchema> lst = null;
        try {
          lst = MetaStoreUtils.getFieldsFromDeserializer(tableName, td.getDeserializer());
        } catch (Exception e) {
          LOG.warn("Error getting schema: "
              + org.apache.hadoop.util.StringUtils.stringifyException(e));
        }
        if (lst != null) {
          schema = new Schema(lst, null);
        }
      }
    }
    if (schema == null) {
      schema = new Schema();
    }
    LOG.info("Returning Hive schema: " + schema);
    return schema;
  }

  /**
   * Get a Schema with fields represented with Thrift DDL types
   */
  public Schema getThriftSchema() throws Exception {
    Schema schema;
    try {
      schema = getSchema();
      if (schema != null) {
        List<FieldSchema> lst = schema.getFieldSchemas();
        // Go over the schema and convert type to thrift type
        if (lst != null) {
          for (FieldSchema f : lst) {
            f.setType(MetaStoreUtils.typeToThriftType(f.getType()));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning Thrift schema: " + schema);
    return schema;
  }

  /**
   * Return the maximum number of rows returned by getResults
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Set the maximum number of rows returned by getResults
   */
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public boolean hasReduceTasks(List<Task<? extends Serializable>> tasks) {
    if (tasks == null) {
      return false;
    }

    boolean hasReduce = false;
    for (Task<? extends Serializable> task : tasks) {
      if (task.hasReduce()) {
        return true;
      }

      hasReduce = (hasReduce || hasReduceTasks(task.getChildTasks()));
    }
    return hasReduce;
  }

  /**
   * for backwards compatibility with current tests
   */
  public Driver(HiveConf conf) {
    this.conf = conf;
  }

  public Driver() {
    if (SessionState.get() != null) {
      conf = SessionState.get().getConf();
    }
  }

  public int compile(String command) {
    return compile(command, false, true);
  }

  /**
   * Compile a new query. Any currently-planned query associated with this Driver is discarded.
   *
   * @param command
   *          The SQL query to compile.
   */
  public int compile(String command, boolean run) {
    return compile(command, run, true);
  }

  /**
   * Hold state variables specific to each query being executed, that may not
   * be consistent in the overall SessionState
   */
  private static class QueryState {
    private HiveOperation op;
    private String cmd;
    private boolean init = false;

    /**
     * Initialize the queryState with the query state variables
     */
    public void init(HiveOperation op, String cmd) {
      this.op = op;
      this.cmd = cmd;
      this.init = true;
    }

    public boolean isInitialized() {
      return this.init;
    }

    public HiveOperation getOp() {
      return this.op;
    }

    public String getCmd() {
      return this.cmd;
    }
  }

  public void saveSession(QueryState qs) {
    SessionState oldss = SessionState.get();
    if (oldss != null && oldss.getHiveOperation() != null) {
      qs.init(oldss.getHiveOperation(), oldss.getCmd());
    }
  }

  public void restoreSession(QueryState qs) {
    SessionState ss = SessionState.get();
    if (ss != null && qs != null && qs.isInitialized()) {
      ss.setCmd(qs.getCmd());
      ss.setCommandType(qs.getOp());
    }
  }

  /**
   * Compile a new query, but potentially reset taskID counter.  Not resetting task counter
   * is useful for generating re-entrant QL queries.
   * @param command  The HiveQL query to compile
   * @param resetTaskIds Resets taskID counter if true.
   * @return 0 for ok
   */
  public int compile(String command, boolean run, boolean resetTaskIds) {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.COMPILE);

    //holder for parent command type/string when executing reentrant queries
    QueryState queryState = new QueryState();

    if (plan != null) {
      close();
      plan = null;
    }

    if (resetTaskIds) {
      TaskFactory.resetId();
    }
    saveSession(queryState);

    try {
      command = new VariableSubstitution().substitute(conf,command);
      ctx = new Context(conf);
      ctx.setTryCount(getTryCount());
      ctx.setCmd(command);
      ctx.setHDFSCleanup(true);

      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      List<AbstractSemanticAnalyzerHook> saHooks =
          getHooks(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
                   AbstractSemanticAnalyzerHook.class);

      // Do semantic analysis and plan generation
      if (saHooks != null) {
        HiveSemanticAnalyzerHookContext hookCtx = new HiveSemanticAnalyzerHookContextImpl();
        hookCtx.setConf(conf);
        for (AbstractSemanticAnalyzerHook hook : saHooks) {
          tree = hook.preAnalyze(hookCtx, tree);
        }
        sem.analyze(tree, ctx);
        hookCtx.update(sem);
        for (AbstractSemanticAnalyzerHook hook : saHooks) {
          hook.postAnalyze(hookCtx, sem.getRootTasks());
        }
      } else {
        sem.analyze(tree, ctx);
      }

      LOG.info("Semantic Analysis Completed");

      // validate the plan
      sem.validate();

      long startTime = perfLogger.getStartTime(run ? PerfLogger.DRIVER_RUN : PerfLogger.COMPILE);
      plan = new QueryPlan(command, sem, startTime);

      // test Only - serialize the query plan and deserialize it
      if ("true".equalsIgnoreCase(System.getProperty("test.serialize.qplan"))) {

        String queryPlanFileName = ctx.getLocalScratchDir(true) + Path.SEPARATOR_CHAR
            + "queryplan.xml";
        LOG.info("query plan = " + queryPlanFileName);
        queryPlanFileName = new Path(queryPlanFileName).toUri().getPath();

        // serialize the queryPlan
        FileOutputStream fos = new FileOutputStream(queryPlanFileName);
        Utilities.serializeQueryPlan(plan, fos);
        fos.close();

        // deserialize the queryPlan
        FileInputStream fis = new FileInputStream(queryPlanFileName);
        QueryPlan newPlan = Utilities.deserializeQueryPlan(fis, conf);
        fis.close();

        // Use the deserialized plan
        plan = newPlan;
      }

      // initialize FetchTask right here
      if (plan.getFetchTask() != null) {
        plan.getFetchTask().initialize(conf, plan, null);
      }

      // get the output schema
      schema = getSchema(sem, conf);

      //do the authorization check
      if (!sem.skipAuthorization() &&
          HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {

        boolean grantAuth = HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVE_AUTHORIZATION_GRANT_ENABLED);
        try {
          perfLogger.PerfLogBegin(LOG, PerfLogger.DO_AUTHORIZATION);
          doAuthorization(sem, grantAuth,
              new AuthorizationFactory.DefaultAuthorizationExceptionHandler());
        } catch (AuthorizationException authExp) {
          console.printError("Authorization failed:" + authExp.getMessage()
              + ". Use SHOW GRANT to get more details.");
          errorMessage = authExp.getMessage();
          SQLState = "01542";
          return 403;
        } finally {
          perfLogger.PerfLogEnd(LOG, PerfLogger.DO_AUTHORIZATION);
        }
      }

      return 0;
    } catch (Exception e) {
      ErrorMsg error = ErrorMsg.getErrorMsg(e.getMessage());
      errorMessage = "FAILED: " + e.getClass().getSimpleName();
      if (error != ErrorMsg.GENERIC_ERROR) {
        errorMessage += " [Error "  + error.getErrorCode()  + "]:";
      }
      errorMessage += " " + e.getMessage();
      SQLState = error.getSQLState();
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return error.getErrorCode();
    } finally {
      //restore state after we're done executing a specific query
      perfLogger.PerfLogEnd(LOG, PerfLogger.COMPILE);
      restoreSession(queryState);
    }
  }

  public static void doAuthorization(BaseSemanticAnalyzer sem, boolean grantAuth,
      AuthorizationFactory.AuthorizationExceptionHandler handler)
      throws HiveException, AuthorizationException {
    HashSet<ReadEntity> inputs = sem.getInputs();
    HashSet<WriteEntity> outputs = sem.getOutputs();

    HiveOperation op = sem.getHiveOperation();
    if (op == null) {
      throw new IllegalStateException("Operation is null");
    }

    Hive db = sem.getDb();
    Privilege[] opInputPrivs = op.getInputRequiredPrivileges();
    Privilege[] opOutputPrivs = op.getOutputRequiredPrivileges();

    boolean grantedOnly = grantAuth &&
        (op.equals(HiveOperation.GRANT_PRIVILEGE) || op.equals(HiveOperation.REVOKE_PRIVILEGE));

    SessionState ss = SessionState.get();
    DelegatableAuthorizationProvider authorizer =
        AuthorizationFactory.create(ss.getAuthorizer(), handler);
    if (grantedOnly) {
      List<Task<?>> tasks = sem.getRootTasks();
      assert tasks.size() == 1 && tasks.get(0) instanceof DDLTask;

      DDLTask task = (DDLTask) tasks.get(0);

      List<PrivilegeDesc> privs;
      PrivilegeObjectDesc subject;
      if (op.equals(HiveOperation.GRANT_PRIVILEGE)) {
        privs = task.getWork().getGrantDesc().getPrivileges();
        subject = task.getWork().getGrantDesc().getPrivilegeSubjectDesc();
      } else {
        privs = task.getWork().getRevokeDesc().getPrivileges();
        subject = task.getWork().getRevokeDesc().getPrivilegeSubjectDesc();
      }
      opInputPrivs = new Privilege[privs.size()];
      opOutputPrivs = new Privilege[privs.size()];
      for (int i = 0; i < privs.size(); i++) {
        opInputPrivs[i] = opOutputPrivs[i] = privs.get(i).getPrivilege();
      }
      if (subject == null) {
        authorizer.authorize(opInputPrivs, opOutputPrivs, grantedOnly);
      }
    }

    if (op.equals(HiveOperation.CREATEDATABASE)) {
      authorizer.authorize(opInputPrivs, opOutputPrivs, grantedOnly);
    }

    if (outputs != null && outputs.size() > 0) {
      for (WriteEntity write : outputs) {
        if (write.isDummy()) {
          continue;
        }
        Privilege[] outputPrivs = write.getOutputRequiredPrivileges();
        if (outputPrivs == null) {
          outputPrivs = opOutputPrivs;
        }
        if (write.getType() == Entity.Type.DATABASE) {
          authorizer.authorize(write.getDatabase(), null, outputPrivs, grantedOnly);
          continue;
        }
        if (write.getType() == Entity.Type.DFS_DIR || write.getType() == Entity.Type.LOCAL_DIR) {
          continue;
        }

        if (write.getType() == WriteEntity.Type.PARTITION) {
          Partition part = db.getPartition(write.getTable(), write
              .getPartition().getSpec(), false);
          if (part != null) {
            authorizer.authorize(write.getPartition(), null,
                outputPrivs, grantedOnly);
            continue;
          }
        }

        if (write.getTable() != null) {
          authorizer.authorize(write.getTable(), null,
              outputPrivs, grantedOnly);
        }
      }

    }

    if (inputs != null && inputs.size() > 0) {

      Map<Table, List<String>> tab2Cols = new HashMap<Table, List<String>>();
      Map<Partition, List<String>> part2Cols = new HashMap<Partition, List<String>>();

      Map<String, Boolean> tableUsePartLevelAuth = new HashMap<String, Boolean>();
      for (ReadEntity read : inputs) {
        if (read.isDummy() ||
            (read.getType() != Entity.Type.TABLE && read.getType() != Entity.Type.PARTITION)) {
          continue;
        }
        Table tbl = read.getTable();
        if ((read.getPartition() != null) || (tbl.isPartitioned())) {
          String tblName = tbl.getTableName();
          if (tableUsePartLevelAuth.get(tblName) == null) {
            boolean usePartLevelPriv = (tbl.getParameters().get(
                "PARTITION_LEVEL_PRIVILEGE") != null && ("TRUE"
                .equalsIgnoreCase(tbl.getParameters().get(
                    "PARTITION_LEVEL_PRIVILEGE"))));
            if (usePartLevelPriv) {
              tableUsePartLevelAuth.put(tblName, Boolean.TRUE);
            } else {
              tableUsePartLevelAuth.put(tblName, Boolean.FALSE);
            }
          }
        }
      }

      if (op.equals(HiveOperation.CREATETABLE_AS_SELECT)
          || op.equals(HiveOperation.QUERY)) {
        SemanticAnalyzer querySem = (SemanticAnalyzer) sem;
        ParseContext parseCtx = querySem.getParseContext();
        Map<TableScanOperator, Table> tsoTopMap = parseCtx.getTopToTable();

        for (Map.Entry<String, Operator<? extends OperatorDesc>> topOpMap : querySem
            .getParseContext().getTopOps().entrySet()) {
          Operator<? extends OperatorDesc> topOp = topOpMap.getValue();
          if (topOp instanceof TableScanOperator
              && tsoTopMap.containsKey(topOp)) {
            TableScanOperator tableScanOp = (TableScanOperator) topOp;
            Table tbl = tsoTopMap.get(tableScanOp);
            List<Integer> neededColumnIds = tableScanOp.getNeededColumnIDs();
            List<FieldSchema> columns = tbl.getCols();
            List<String> cols = new ArrayList<String>();
            if (neededColumnIds != null && neededColumnIds.size() > 0) {
              for (int i = 0; i < neededColumnIds.size(); i++) {
                cols.add(columns.get(neededColumnIds.get(i)).getName());
              }
            } else {
              for (int i = 0; i < columns.size(); i++) {
                cols.add(columns.get(i).getName());
              }
            }
            //map may not contain all sources, since input list may have been optimized out
            //or non-existent tho such sources may still be referenced by the TableScanOperator
            //if it's null then the partition probably doesn't exist so let's use table permission
            if (tbl.isPartitioned() &&
                tableUsePartLevelAuth.get(tbl.getTableName()) == Boolean.TRUE) {
              String alias_id = topOpMap.getKey();
              PrunedPartitionList partsList = PartitionPruner.prune(parseCtx
                  .getTopToTable().get(topOp), parseCtx.getOpToPartPruner()
                  .get(topOp), parseCtx.getConf(), alias_id, parseCtx
                  .getPrunedPartitions());
              Set<Partition> parts = new HashSet<Partition>();
              parts.addAll(partsList.getConfirmedPartns());
              parts.addAll(partsList.getUnknownPartns());
              for (Partition part : parts) {
                List<String> existingCols = part2Cols.get(part);
                if (existingCols == null) {
                  existingCols = new ArrayList<String>();
                }
                existingCols.addAll(cols);
                part2Cols.put(part, existingCols);
              }
            } else {
              List<String> existingCols = tab2Cols.get(tbl);
              if (existingCols == null) {
                existingCols = new ArrayList<String>();
              }
              existingCols.addAll(cols);
              tab2Cols.put(tbl, existingCols);
            }
          }
        }
      }

      // cache the results for table authorization
      Set<String> tableAuthChecked = new HashSet<String>();
      for (ReadEntity read : inputs) {
        if (read.isDummy()) {
          continue;
        }
        Privilege[] inputPrivs = read.getInputRequiredPrivileges();
        if (inputPrivs == null) {
          inputPrivs = opInputPrivs;
        }
        HiveAuthorizationProvider delegator = authorizer.authorizeFor(read);

        if (read.getType() == Entity.Type.DATABASE) {
          delegator.authorize(read.getDatabase(), inputPrivs, null, grantedOnly);
          continue;
        }
        if (read.getType() == Entity.Type.DFS_DIR || read.getType() == Entity.Type.LOCAL_DIR) {
          continue;
        }
        Table tbl = read.getTable();
        if (read.getPartition() != null) {
          Partition partition = read.getPartition();
          tbl = partition.getTable();
          // use partition level authorization
          if (tableUsePartLevelAuth.get(tbl.getTableName()) == Boolean.TRUE) {
            List<String> cols = part2Cols.get(partition);
            if (cols != null && cols.size() > 0) {
              delegator.authorize(partition.getTable(),
                  partition, cols, inputPrivs,
                  null, grantedOnly);
            } else {
              delegator.authorize(partition,
                  inputPrivs, null, grantedOnly);
            }
            continue;
          }
        }

        // if we reach here, it means it needs to do a table authorization
        // check, and the table authorization may already happened because of other
        // partitions
        if (tbl != null && !tableAuthChecked.contains(tbl.getTableName()) &&
            !(tableUsePartLevelAuth.get(tbl.getTableName()) == Boolean.TRUE)) {
          List<String> cols = tab2Cols.get(tbl);
          if (cols != null && cols.size() > 0) {
            delegator.authorize(tbl, null, cols,
                inputPrivs, null, grantedOnly);
          } else {
            delegator.authorize(tbl, inputPrivs,
                null, grantedOnly);
          }
          tableAuthChecked.add(tbl.getTableName());
        }
      }

    }
  }

  /**
   * @return The current query plan associated with this Driver, if any.
   */
  public QueryPlan getPlan() {
    return plan;
  }

  /**
   * @param t
   *          The table to be locked
   * @param p
   *          The partition to be locked
   * @param mode
   *          The mode of the lock (SHARED/EXCLUSIVE) Get the list of objects to be locked. If a
   *          partition needs to be locked (in any mode), all its parents should also be locked in
   *          SHARED mode.
   */
  private List<HiveLockObj> toLockObjects(
      Database d, Table t, Partition p, HiveLockMode mode, HiveLockObjectData lockData) {

    List<HiveLockObj> locks = new ArrayList<HiveLockObj>();
    if (d != null) {
      locks.add(new HiveLockObj(new HiveLockObject(d, lockData), mode));
    } else if (t != null) {
      locks.add(new HiveLockObj(new HiveLockObject(t.getDbName(), lockData), HiveLockMode.SHARED));
      locks.add(new HiveLockObj(new HiveLockObject(t, lockData), mode));
    } else if (p != null) {
      t = p.getTable();
      locks.add(new HiveLockObj(new HiveLockObject(t.getDbName(), lockData), HiveLockMode.SHARED));
      if (p instanceof DummyPartition && p.getSpec().isEmpty()) {
        locks.add(new HiveLockObj(new HiveLockObject(t, lockData), mode));
        return locks;
      }
      locks.add(new HiveLockObj(new HiveLockObject(t, lockData), HiveLockMode.SHARED));

      String name = p.getName();
      // For dummy partitions, only partition name is needed
      if (p instanceof DummyPartition) {
        name = name.split("@")[2];
      }
      String[] partns = name.split("/");

      List<String> interm = new ArrayList<String>();
      interm.add(t.getDbName());
      interm.add(t.getTableName());
      for (int idx = 0; idx < partns.length; idx++) {
        interm.add(partns[idx]);
        String[] paths = interm.toArray(new String[interm.size()]);
        locks.add(new HiveLockObj(new HiveLockObject(paths, lockData),
            idx == partns.length - 1 ? mode : HiveLockMode.SHARED));
      }
    }
    return locks;
  }

  private void addLock(HiveLockObj lockObj, Map<HiveLockObject, HiveLockMode> lockObjects) {
    HiveLockMode prevMode = lockObjects.get(lockObj.getObj());
    if (prevMode == null) {
      lockObjects.put(lockObj.getObj(), lockObj.getMode());
    } else if (prevMode == HiveLockMode.SHARED && lockObj.getMode() == HiveLockMode.EXCLUSIVE) {
      lockObjects.put(lockObj.getObj(), HiveLockMode.EXCLUSIVE);
    }
  }

  /**
   * Acquire read and write locks needed by the statement. The list of objects to be locked are
   * obtained from he inputs and outputs populated by the compiler. The lock acuisition scheme is
   * pretty simple. If all the locks cannot be obtained, error out. Deadlock is avoided by making
   * sure that the locks are lexicographically sorted.
   **/
  private int acquireReadWriteLocks() {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);

    try {
      boolean supportConcurrency = conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
      if (!supportConcurrency) {
        return 0;
      }

      HiveLockObjectData lockData =
          new HiveLockObjectData(plan.getQueryId(),
              String.valueOf(System.currentTimeMillis()),
              "IMPLICIT",
              plan.getQueryStr());

      Map<HiveLockObject, HiveLockMode> deduped = new HashMap<HiveLockObject, HiveLockMode>();

      // Sort all the inputs, outputs.
      // If a lock needs to be acquired on any partition, a read lock needs to be acquired on all
      // its parents also
      for (ReadEntity input : plan.getInputs()) {
        List<HiveLockObj> locks = Collections.emptyList();
        if (input.getType() == ReadEntity.Type.DATABASE) {
          locks = toLockObjects(input.getDatabase(), null, null, HiveLockMode.SHARED, lockData);
        } else if (input.getType() == ReadEntity.Type.TABLE) {
          locks = toLockObjects(null, input.getTable(), null, HiveLockMode.SHARED, lockData);
        } else if (input.getType() == WriteEntity.Type.PARTITION) {
          locks = toLockObjects(null, null, input.getPartition(), HiveLockMode.SHARED, lockData);
        }
        for (HiveLockObj lock : locks) {
          addLock(lock, deduped);
        }
      }

      for (WriteEntity output : plan.getOutputs()) {
        List<HiveLockObj> locks = Collections.emptyList();
        if (output.getType() == WriteEntity.Type.DATABASE) {
          locks = toLockObjects(output.getDatabase(), null, null, HiveLockMode.EXCLUSIVE, lockData);
        } else if (output.getType() == WriteEntity.Type.TABLE) {
          locks = toLockObjects(null, output.getTable(), null, HiveLockMode.EXCLUSIVE, lockData);
        } else if (output.getType() == WriteEntity.Type.PARTITION) {
          locks = toLockObjects(null, null, output.getPartition(), HiveLockMode.EXCLUSIVE, lockData);
        } else if (output.getType() == WriteEntity.Type.DUMMYPARTITION) {
          // In case of dynamic queries, it is possible to have incomplete dummy partitions
          locks = toLockObjects(null, null, output.getPartition(), HiveLockMode.EXCLUSIVE, lockData);
        }
        for (HiveLockObj lock : locks) {
          addLock(lock, deduped);
        }
        ctx.getOutputLockObjects().put(output, locks);
      }

      if (deduped.isEmpty() && !ctx.isNeedLockMgr()) {
        return 0;
      }

      List<HiveLockObj> locks = new ArrayList<HiveLockObj>();
      for (Map.Entry<HiveLockObject, HiveLockMode> entry : deduped.entrySet()) {
        locks.add(new HiveLockObj(entry.getKey(), entry.getValue()));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Locking " + locks);
      }
      List<HiveLock> hiveLocks = ctx.getHiveLockMgr().lock(locks, false);
      if (hiveLocks == null) {
        throw new SemanticException(ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg());
      }
      ctx.setHiveLocks(hiveLocks);

      return 0;
    } catch (SemanticException e) {
      errorMessage = "FAILED: Error in acquiring locks: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 10;
    } catch (Exception e) {
      errorMessage = "FAILED: Error in acquiring locks: " + e.getMessage();
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 11;
    } finally {
      perfLogger.PerfLogEnd(LOG, PerfLogger.ACQUIRE_READ_WRITE_LOCKS);
    }
  }

  /**
   * @param hiveLocks
   *          list of hive locks to be released Release all the locks specified. If some of the
   *          locks have already been released, ignore them
   **/
  private void releaseLocks(List<HiveLock> hiveLocks) {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.RELEASE_LOCKS);

    if (hiveLocks != null) {
      HiveOperation operation =
        SessionState.get() != null ? SessionState.get().getHiveOperation() : null;
      boolean recursive =
        operation == HiveOperation.DROPTABLE || operation == HiveOperation.DROPDATABASE;
      ctx.getHiveLockMgr().releaseLocks(hiveLocks, recursive);
    }
    ctx.setHiveLocks(null);

    perfLogger.PerfLogEnd(LOG, PerfLogger.RELEASE_LOCKS);
  }

  public CommandProcessorResponse run(String command) throws CommandNeedRetryException {
    CommandProcessorResponse response = compileCommand(command, true);
    if (response.getResponseCode() != 0) {
      return response;
    }
    return executePlan(true);
  }

  public CommandProcessorResponse compileCommand(String command, boolean run) {
    errorMessage = null;
    SQLState = null;

    driverRunHooks = null;
    hookContext = null;

    if (!validateConfVariables()) {
      return new CommandProcessorResponse(12, errorMessage, SQLState);
    }

    hookContext = new HiveDriverRunHookContextImpl(conf, command);
    // Get all the driver run hooks and pre-execute them.
    try {
      driverRunHooks = getHooks(HiveConf.ConfVars.HIVE_DRIVER_RUN_HOOKS, HiveDriverRunHook.class);
      for (HiveDriverRunHook driverRunHook : driverRunHooks) {
        driverRunHook.preDriverRun(hookContext);
      }
    } catch (Exception e) {
      errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return new CommandProcessorResponse(12, errorMessage, SQLState);
    }

    // Reset the perf logger
    if (run) {
      PerfLogger perfLogger = PerfLogger.getPerfLogger(true);
      perfLogger.PerfLogBegin(LOG, PerfLogger.DRIVER_RUN);
      perfLogger.PerfLogBegin(LOG, PerfLogger.TIME_TO_SUBMIT);
    }

    int ret;
    synchronized (compileMonitor) {
      ret = compile(command, run);
    }
    if (ret != 0) {
      return new CommandProcessorResponse(ret, errorMessage, SQLState);
    }
    return new CommandProcessorResponse(ret);
  }

  public CommandProcessorResponse executePlan(boolean run) throws CommandNeedRetryException {

    int ret;
    boolean requireLock = false;
    boolean ckLock = checkLockManager();

    if (ckLock) {
      boolean lockOnlyMapred = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_LOCK_MAPRED_ONLY);
      if(lockOnlyMapred) {
        Queue<Task<? extends Serializable>> taskQueue = new LinkedList<Task<? extends Serializable>>();
        taskQueue.addAll(plan.getRootTasks());
        while (taskQueue.peek() != null) {
          Task<? extends Serializable> tsk = taskQueue.remove();
          requireLock = requireLock || tsk.requireLock();
          if(requireLock) {
            break;
          }
          if (tsk instanceof ConditionalTask) {
            taskQueue.addAll(((ConditionalTask)tsk).getListTasks());
          }
          if(tsk.getChildTasks()!= null) {
            taskQueue.addAll(tsk.getChildTasks());
          }
          // does not add back up task here, because back up task should be the same
          // type of the original task.
        }
      } else {
        requireLock = true;
      }
    }

    if (requireLock) {
      ret = acquireReadWriteLocks();
      if (ret != 0) {
        return new CommandProcessorResponse(ret, errorMessage, SQLState);
      }
    }

    try {
      ret = execute(run);
    } finally {
      releaseLocks(ctx.getHiveLocks());
    }
    if (ret != 0) {
      return new CommandProcessorResponse(ret, errorMessage, SQLState);
    }

    PerfLogger perfLogger = PerfLogger.getPerfLogger(true);
    if (run) {
      perfLogger.PerfLogEnd(LOG, PerfLogger.DRIVER_RUN);
    }
    perfLogger.close(LOG, plan);

    // Take all the driver run hooks and post-execute them.
    try {
      for (HiveDriverRunHook driverRunHook : driverRunHooks) {
        driverRunHook.postDriverRun(hookContext);
      }
    } catch (Exception e) {
      errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      SQLState = ErrorMsg.findSQLState(e.getMessage());
      console.printError(errorMessage + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return new CommandProcessorResponse(12, errorMessage, SQLState);
    }

    return new CommandProcessorResponse(ret);
  }

  /**
   * Validate configuration variables.
   *
   * @return
   */
  private boolean validateConfVariables() {
    boolean valid = true;
    if ((!conf.getBoolVar(HiveConf.ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES))
        && ((conf.getBoolVar(HiveConf.ConfVars.HADOOPMAPREDINPUTDIRRECURSIVE)) || (conf
              .getBoolVar(HiveConf.ConfVars.HIVEOPTLISTBUCKETING)) || ((conf
                  .getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_UNION_REMOVE))))) {
      errorMessage = "FAILED: Hive Internal Error: "
          + ErrorMsg.SUPPORT_DIR_MUST_TRUE_FOR_LIST_BUCKETING.getMsg();
      SQLState = ErrorMsg.findSQLState(errorMessage);
      console.printError(errorMessage + "\n");
      valid = false;
    }
    return valid;
  }

  /**
   * Returns a set of hooks specified in a configuration variable.
   *
   * See getHooks(HiveConf.ConfVars hookConfVar, Class<T> clazz)
   * @param hookConfVar
   * @return
   * @throws Exception
   */
  private List<Hook> getHooks(HiveConf.ConfVars hookConfVar) throws Exception {
    return getHooks(hookConfVar, Hook.class);
  }

  /**
   * Returns the hooks specified in a configuration variable.  The hooks are returned in a list in
   * the order they were specified in the configuration variable.
   *
   * @param hookConfVar The configuration variable specifying a comma separated list of the hook
   *                    class names.
   * @param clazz       The super type of the hooks.
   * @return            A list of the hooks cast as the type specified in clazz, in the order
   *                    they are listed in the value of hookConfVar
   * @throws Exception
   */
  private <T extends Hook> List<T> getHooks(HiveConf.ConfVars hookConfVar, Class<T> clazz)
      throws Exception {

    List<T> hooks = new ArrayList<T>();
    String csHooks = conf.getVar(hookConfVar);
    if (csHooks == null) {
      return hooks;
    }

    csHooks = csHooks.trim();
    if (csHooks.equals("")) {
      return hooks;
    }

    String[] hookClasses = csHooks.split(",");

    for (String hookClass : hookClasses) {
      try {
        T hook =
            (T) Class.forName(hookClass.trim(), true, JavaUtils.getClassLoader()).newInstance();
        hooks.add(hook);
      } catch (ClassNotFoundException e) {
        console.printError(hookConfVar.varname + " Class not found:" + e.getMessage());
        throw e;
      }
    }

    return hooks;
  }

  public int execute(boolean run) throws CommandNeedRetryException {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.DRIVER_EXECUTE);

    boolean noName = StringUtils.isEmpty(conf.getVar(HiveConf.ConfVars.HADOOPJOBNAME));
    int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);

    String queryId = plan.getQueryId();
    String queryStr = plan.getQueryStr();

    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, queryId);
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, queryStr);

    conf.set("mapreduce.workflow.id", "hive_"+queryId);
    conf.set("mapreduce.workflow.name", queryStr);

    maxthreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.EXECPARALLETHREADNUMBER);

    try {
      LOG.info("Starting command: " + queryStr);

      plan.setStarted();

      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().startQuery(queryStr,
            conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
        SessionState.get().getHiveHistory().logPlanProgress(plan);
      }
      resStream = null;

      HookContext hookContext = new HookContext(plan, conf, ctx.getPathToCS());
      hookContext.setHookType(HookContext.HookType.PRE_EXEC_HOOK);

      for (Hook peh : getHooks(HiveConf.ConfVars.PREEXECHOOKS)) {
        if (peh instanceof ExecuteWithHookContext) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());

          ((ExecuteWithHookContext) peh).run(hookContext);

          perfLogger.PerfLogEnd(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());
        } else if (peh instanceof PreExecute) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());

          ((PreExecute) peh).run(SessionState.get(), plan.getInputs(), plan.getOutputs(),
              ShimLoader.getHadoopShims().getUGIForConf(conf));

          perfLogger.PerfLogEnd(LOG, PerfLogger.PRE_HOOK + peh.getClass().getName());
        }
      }


      int jobs = Utilities.getMRTasks(plan.getRootTasks()).size();
      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
      }
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_NUM_TASKS,
            String.valueOf(jobs));
        SessionState.get().getHiveHistory().setIdToTableMap(plan.getIdToTableNameMap());
      }
      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      // A runtime that launches runnable tasks as separate Threads through
      // TaskRunners
      // As soon as a task isRunnable, it is put in a queue
      // At any time, at most maxthreads tasks can be running
      // The main thread polls the TaskRunners to check if they have finished.

      DriverContext driverCxt = new DriverContext(ctx);
      ctx.setHDFSCleanup(true);

      this.driverCxt = driverCxt; // for canceling the query (should be bound to session?)

      SessionState.get().setLastMapRedStatsList(new ArrayList<MapRedStats>());
      SessionState.get().setStackTraces(new HashMap<String, List<List<String>>>());
      SessionState.get().setLocalMapRedErrors(new HashMap<String, List<String>>());

      if (plan.isUsingPseudoMR()) {
        for (Task<? extends Serializable> tsk : plan.getRootTasks()) {
          tsk.initialize(conf, plan, driverCxt);
        }
      }
      // Add root Tasks to runnable
      for (Task<? extends Serializable> tsk : plan.getRootTasks()) {
        // This should never happen, if it does, it's a bug with the potential to produce
        // incorrect results.
        assert tsk.getParentTasks() == null || tsk.getParentTasks().isEmpty();
        driverCxt.addToRunnable(tsk);
      }

      if (run) {
        perfLogger.PerfLogEnd(LOG, PerfLogger.TIME_TO_SUBMIT);
      }
      // Loop while you either have tasks running, or tasks queued up
      while (!destroyed && driverCxt.isRunning()) {

        // Launch upto maxthreads tasks
        Task<? extends Serializable> task;
        while ((task = driverCxt.getRunnable(maxthreads)) != null) {
          TaskRunner runner = launchTask(task, queryId, noName, jobname, jobs, driverCxt);
          if (!runner.isRunning()) {
            break;
          }
        }

        // poll the Tasks to see which one completed
        TaskRunner tskRun = driverCxt.pollFinished();
        if (tskRun == null) {
          continue;
        }
        hookContext.addCompleteTask(tskRun);

        Task<? extends Serializable> tsk = tskRun.getTask();
        TaskResult result = tskRun.getTaskResult();

        int exitVal = result.getExitVal();
        if (exitVal != 0) {
          if (tsk.ifRetryCmdWhenFail()) {
            driverCxt.shutdown();
            // in case we decided to run everything in local mode, restore the
            // the jobtracker setting to its initial value
            ctx.restoreOriginalTracker();
            throw new CommandNeedRetryException();
          }
          Task<? extends Serializable> backupTask = tsk.getAndInitBackupTask();
          if (backupTask != null) {
            errorMessage = "FAILED: Execution Error, return code " + exitVal + " from "
                + tsk.getClass().getName();
            ErrorMsg em = ErrorMsg.getErrorMsg(exitVal);
            if (em != null) {
              errorMessage += ". " +  em.getMsg();
            }
            console.printError(errorMessage);
            errorMessage = "ATTEMPT: Execute BackupTask: " + backupTask.getClass().getName();
            console.printError(errorMessage);

            // add backup task to runnable
            if (DriverContext.isLaunchable(backupTask)) {
              driverCxt.addToRunnable(backupTask);
            }
            continue;

          } else {
            hookContext.setHookType(HookContext.HookType.ON_FAILURE_HOOK);
            // Get all the failure execution hooks and execute them.
            for (Hook ofh : getHooks(HiveConf.ConfVars.ONFAILUREHOOKS)) {
              perfLogger.PerfLogBegin(LOG, PerfLogger.FAILURE_HOOK + ofh.getClass().getName());

              ((ExecuteWithHookContext) ofh).run(hookContext);

              perfLogger.PerfLogEnd(LOG, PerfLogger.FAILURE_HOOK + ofh.getClass().getName());
            }

            errorMessage = "FAILED: Execution Error, return code " + exitVal + " from "
                + tsk.getClass().getName();
            ErrorMsg em = ErrorMsg.getErrorMsg(exitVal);
            if (em != null) {
              errorMessage += ". " +  em.getMsg();
            } else if (result.getErrorMsg() != null) {
              errorMessage += ". " +  result.getErrorMsg();
            }
            SQLState = "08S01";
            console.printError(errorMessage);
            driverCxt.shutdown();
            // in case we decided to run everything in local mode, restore the
            // the jobtracker setting to its initial value
            ctx.restoreOriginalTracker();
            return exitVal;
          }
        }

        if (SessionState.get() != null) {
          SessionState.get().getHiveHistory().setTaskProperty(queryId, tsk.getId(),
              Keys.TASK_RET_CODE, String.valueOf(exitVal));
          SessionState.get().getHiveHistory().endTask(queryId, tsk);
        }

        if (tsk.getChildTasks() != null) {
          for (Task<? extends Serializable> child : tsk.getChildTasks()) {
            if (DriverContext.isLaunchable(child)) {
              driverCxt.addToRunnable(child);
            }
          }
        }
      }
      // in case we decided to run everything in local mode, restore the
      // the jobtracker setting to its initial value
      ctx.restoreOriginalTracker();

      if (driverCxt.isShutdown()) {
        SQLState = "HY008";
        errorMessage = "FAILED: Operation cancelled";
        console.printError(errorMessage);
        return 1000;
      }

      hookContext.setHookType(HookContext.HookType.POST_EXEC_HOOK);
      // Get all the post execution hooks and execute them.
      for (Hook peh : getHooks(HiveConf.ConfVars.POSTEXECHOOKS)) {
        if (peh instanceof ExecuteWithHookContext) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());

          ((ExecuteWithHookContext) peh).run(hookContext);

          perfLogger.PerfLogEnd(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());
        } else if (peh instanceof PostExecute) {
          perfLogger.PerfLogBegin(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());

          ((PostExecute) peh).run(SessionState.get(), plan.getInputs(), plan.getOutputs(),
              (SessionState.get() != null ? SessionState.get().getLineageState().getLineageInfo()
                  : null), ShimLoader.getHadoopShims().getUGIForConf(conf));

          perfLogger.PerfLogEnd(LOG, PerfLogger.POST_HOOK + peh.getClass().getName());
        }
      }


      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(0));
        SessionState.get().getHiveHistory().printRowCount(queryId);
      }
    } catch (CommandNeedRetryException e) {
      throw e;
    } catch (Exception e) {
      ctx.restoreOriginalTracker();
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setQueryProperty(queryId, Keys.QUERY_RET_CODE,
            String.valueOf(12));
      }
      // TODO: do better with handling types of Exception here
      errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
      SQLState = "08S01";
      console.printError(errorMessage + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 12;
    } finally {
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().endQuery(queryId);
      }
      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, "");
      }
      perfLogger.PerfLogEnd(LOG, PerfLogger.DRIVER_EXECUTE);

      if (SessionState.get().getLastMapRedStatsList() != null
          && SessionState.get().getLastMapRedStatsList().size() > 0) {
        long totalCpu = 0;
        console.printInfo("MapReduce Jobs Launched: ");
        for (int i = 0; i < SessionState.get().getLastMapRedStatsList().size(); i++) {
          console.printInfo("Job " + i + ": " + SessionState.get().getLastMapRedStatsList().get(i));
          totalCpu += SessionState.get().getLastMapRedStatsList().get(i).getCpuMSec();
        }
        console.printInfo("Total MapReduce CPU Time Spent: " + Utilities.formatMsecToStr(totalCpu));
      }
    }
    plan.setDone();

    if (SessionState.get() != null) {
      try {
        SessionState.get().getHiveHistory().logPlanProgress(plan);
      } catch (Exception e) {
        // ignore
      }
    }
    console.printInfo("OK");

    return (0);
  }

  /**
   * Launches a new task
   *
   * @param tsk
   *          task being launched
   * @param queryId
   *          Id of the query containing the task
   * @param noName
   *          whether the task has a name set
   * @param running
   *          map from taskresults to taskrunners
   * @param jobname
   *          name of the task, if it is a map-reduce job
   * @param jobs
   *          number of map-reduce jobs
   * @param cxt
   *          the driver context
   */
  private TaskRunner launchTask(Task<? extends Serializable> tsk, String queryId, boolean noName,
      String jobname, int jobs, DriverContext cxt) {
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().startTask(queryId, tsk, tsk.getClass().getName());
    }
    if (tsk.isMapRedTask() && !(tsk instanceof ConditionalTask)) {
      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname + "(" + tsk.getId() + ")");
      }
      conf.set("mapreduce.workflow.node.name", tsk.getId());
      Utilities.setWorkflowAdjacencies(conf, plan);
      cxt.incCurJobNo(1);
      console.printInfo("Launching Job " + cxt.getCurJobNo() + " out of " + jobs);
    }
    if (!tsk.getInitialized()) {
      tsk.initialize(conf, plan, cxt);
    }
    TaskResult tskRes = new TaskResult();
    TaskRunner tskRun = new TaskRunner(tsk, tskRes);

    cxt.launching(tskRun);
    // Launch Task
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL) && tsk.isMapRedTask()) {
      // Launch it in the parallel mode, as a separate thread only for MR tasks
      tskRun.start();
    } else {
      tskRun.runSequential();
    }
    return tskRun;
  }

  public boolean isFetchingTable() {
    return plan != null && plan.getFetchTask() != null;
  }

  public boolean getResults(List res) throws IOException, CommandNeedRetryException {
    if (destroyed) {
      throw new AsynchronousCloseException();
    }
    if (isFetchingTable()) {
      FetchTask ft = plan.getFetchTask();
      ft.setMaxRows(maxRows);
      return ft.fetch(res);
    }

    if (resStream == null) {
      resStream = ctx.getStream();
    }
    if (resStream == null) {
      return false;
    }

    int numRows = 0;
    String row = null;

    while (numRows < maxRows) {
      if (resStream == null) {
        if (numRows > 0) {
          return true;
        } else {
          return false;
        }
      }

      bos.reset();
      Utilities.StreamStatus ss;
      try {
        ss = Utilities.readColumn(resStream, bos);
        if (bos.getCount() > 0) {
          row = new String(bos.getData(), 0, bos.getCount(), "UTF-8");
        } else if (ss == Utilities.StreamStatus.TERMINATED) {
          row = new String();
        }

        if (row != null) {
          numRows++;
          res.add(row);
        }
        row = null;
      } catch (IOException e) {
        console.printError("FAILED: Unexpected IO exception : " + e.getMessage());
        res.clear();
        return false;
      }

      if (ss == Utilities.StreamStatus.EOF) {
        resStream = ctx.getStream();
      }
    }
    return true;
  }

  public int getTryCount() {
    return tryCount;
  }

  public void setTryCount(int tryCount) {
    this.tryCount = tryCount;
  }


  public int close() {
    try {
      if (plan != null) {
        FetchTask fetchTask = plan.getFetchTask();
        if (null != fetchTask) {
          try {
            fetchTask.clearFetch();
          } catch (Exception e) {
            LOG.debug(" Exception while clearing the Fetch task ", e);
          }
        }
      }
      if (driverCxt != null) {
        driverCxt.shutdown();
        driverCxt = null;
      }
      if (ctx != null) {
        ctx.clear();
      }
      if (null != resStream) {
        try {
          ((FSDataInputStream) resStream).close();
        } catch (Exception e) {
          LOG.debug(" Exception while closing the resStream ", e);
        }
      }
    } catch (Exception e) {
      console.printError("FAILED: Hive Internal Error: " + Utilities.getNameMessage(e) + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 13;
    }

    return 0;
  }

  public void destroy() {
    if (destroyed) {
      return;
    }
    destroyed = true;
    if (ctx != null) {
      releaseLocks(ctx.getHiveLocks());
    }

    if (hiveLockMgr != null) {
      try {
        hiveLockMgr.close();
      } catch(LockException e) {
        LOG.warn("Exception in closing hive lock manager. "
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }
  }

  public org.apache.hadoop.hive.ql.plan.api.Query getQueryPlan() throws IOException {
    return plan.getQueryPlan();
  }
}
