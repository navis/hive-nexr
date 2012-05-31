package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class ForwardTask extends Task<FetchWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(ForwardTask.class);

  private FetchOperator ftOp;
  private TableScanOperator processor;

  public ForwardTask() {
  }

  public ForwardTask(FetchWork fetch) {
    setWork(fetch);
  }

  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext ctx) {
    super.initialize(conf, queryPlan, ctx);

    try {
      // Create a file system handle
      JobConf job = new JobConf(conf, ExecDriver.class);

      String serdeName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEFETCHOUTPUTSERDE);
      Class<? extends SerDe> serdeClass = Class.forName(serdeName, true,
          JavaUtils.getClassLoader()).asSubclass(SerDe.class);

      processor = (TableScanOperator) work.getProcessor();
      setupTmpDir(processor, job, new HashSet<Operator>());

      ftOp = new FetchOperator(work, processor, job);
      processor.initialize(conf, new ObjectInspector[]{ftOp.getOutputObjectInspector()});
    } catch (Exception e) {
      // Bail out ungracefully - we should never hit
      // this here - but would have hit it in SemanticAnalyzer
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  private void setupTmpDir(Operator<?> parent, JobConf job, Set<Operator> setup) throws IOException {
    if (parent.getChildOperators() == null) {
      return;
    }
    for (Operator<?> operator : parent.getChildOperators()) {
      if (operator instanceof FileSinkOperator && setup.add(operator)) {
         FileSinkDesc fdesc = ((FileSinkOperator) operator).getConf();
        String tempDir = fdesc.getDirName();

        if (tempDir != null) {
          Path tempPath = Utilities.toTempPath(new Path(tempDir));
          LOG.info("Making Temp Directory: " + tempDir);
          FileSystem fs = tempPath.getFileSystem(job);
          fs.mkdirs(tempPath);
        }
      }
      setupTmpDir(operator, job, setup);
    }
  }

  @Override
  public int execute(DriverContext driverContext) {
    try {
      for (InspectableObject io = ftOp.getNextRow(); io != null; io = ftOp.getNextRow()) {
        processor.forward(io.o, io.oi);
      }
      processor.close(false);
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      try {
        processor.close(true);
      } catch (HiveException e1) {
        throw new RuntimeException("Hive Runtime Error while closing operators", e1);
      }
      console.printError("Failed with exception " + e.getMessage(),
          "\n" + StringUtils.stringifyException(e));
      return 1;
    }
  }

  /**
   * Return the tableDesc of the fetchWork.
   */
  public TableDesc getTblDesc() {
    return work.getTblDesc();
  }

  @Override
  public StageType getType() {
    return StageType.FETCH;
  }

  @Override
  public String getName() {
    return "FETCH";
  }

  @Override
  protected void localizeMRTmpFilesImpl(Context ctx) {
    String s = work.getTblDir();
    if ((s != null) && ctx.isMRTmpFileURI(s)) {
      work.setTblDir(ctx.localizeMRTmpFileURI(s));
    }

    ArrayList<String> ls = work.getPartDir();
    if (ls != null) {
      ctx.localizePaths(ls);
    }
  }

  /**
   * Clear the Fetch Operator.
   *
   * @throws HiveException
   */
  public void clearFetch() throws HiveException {
    if (null != ftOp) {
      ftOp.clearFetchContext();
    }
    if (null != work.getProcessor()) {
      work.getProcessor().close(false);
    }
  }
}
