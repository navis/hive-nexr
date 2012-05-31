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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveContextAwareRecordReader;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * FetchTask implementation.
 **/
public class FetchOperator implements Serializable {

  static Log LOG = LogFactory.getLog(FetchOperator.class.getName());
  static LogHelper console = new LogHelper(LOG);

  private boolean isEmptyTable;
  private boolean isNativeTable;
  private FetchWork work;
  private TableScanOperator ts;
  private int splitNum;
  private PartitionDesc currPart;
  private TableDesc currTbl;
  private boolean tblDataDone;
  private boolean hasVirtialColumn;

  private boolean hasVC;
  private boolean isPartitioned;
  private StructObjectInspector vcsOI;
  private ExecMapperContext context;

  private transient RecordReader<WritableComparable, Writable> currRecReader;
  private transient HiveInputFormat.HiveInputSplit[] inputSplits;
  private transient InputFormat inputFormat;
  private transient JobConf job;
  private transient WritableComparable key;
  private transient Writable value;
  private transient Writable[] vcValues;
  private transient Deserializer serde;
  private transient Iterator<Path> iterPath;
  private transient Iterator<PartitionDesc> iterPartDesc;
  private transient Path currPath;
  private transient StructObjectInspector rowObjectInspector;
  private transient Object[] row;

  public FetchOperator() {
  }

  public FetchOperator(FetchWork work, JobConf job) {
    this.job = job;
    this.work = work;
    initialize();
  }

  public FetchOperator(FetchWork work, TableScanOperator ts, JobConf job) {
    this.job = job;
    this.work = work;
    this.ts = ts;
    initialize();
  }

  private void initialize() {
    if (hasVC = ts != null && ts.getConf() != null &&
        ts.getConf().getVirtualCols() != null && !ts.getConf().getVirtualCols().isEmpty()) {
      List<VirtualColumn> vcs = ts.getConf().getVirtualCols();
      List<String> names = new ArrayList<String>(vcs.size());
      List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(vcs.size());
      for (VirtualColumn vc : vcs) {
        inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                vc.getTypeInfo().getPrimitiveCategory()));
        names.add(vc.getName());
      }
      vcsOI = ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
      vcValues = new Writable[vcs.size()];
    }
    isPartitioned = work.getTblDesc() == null;
    tblDataDone = false;
    if (hasVC && isPartitioned) {
      row = new Object[3];
    } else if (hasVC || isPartitioned) {
      row = new Object[2];
    } else {
      row = new Object[1];
    }
    if (work.getTblDesc() != null) {
      isNativeTable = !work.getTblDesc().isNonNative();
    } else {
      isNativeTable = true;
    }
    if (hasVC) {
      ts.setExecContext(context = new ExecMapperContext());
    }
  }

  private boolean hasVirtualColumn(TableScanOperator ts) {
    return ts != null && ts.getConf() != null &&
        ts.getConf().getVirtualCols() != null && !ts.getConf().getVirtualCols().isEmpty();
  }

  public FetchWork getWork() {
    return work;
  }

  public void setWork(FetchWork work) {
    this.work = work;
  }

  public int getSplitNum() {
    return splitNum;
  }

  public void setSplitNum(int splitNum) {
    this.splitNum = splitNum;
  }

  public PartitionDesc getCurrPart() {
    return currPart;
  }

  public void setCurrPart(PartitionDesc currPart) {
    this.currPart = currPart;
  }

  public TableDesc getCurrTbl() {
    return currTbl;
  }

  public void setCurrTbl(TableDesc currTbl) {
    this.currTbl = currTbl;
  }

  public boolean isTblDataDone() {
    return tblDataDone;
  }

  public void setTblDataDone(boolean tblDataDone) {
    this.tblDataDone = tblDataDone;
  }

  public boolean isEmptyTable() {
    return isEmptyTable;
  }

  public void setEmptyTable(boolean isEmptyTable) {
    this.isEmptyTable = isEmptyTable;
  }

  /**
   * A cache of InputFormat instances.
   */
  private static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats = new HashMap<Class, InputFormat<WritableComparable, Writable>>();

  @SuppressWarnings("unchecked")
  static InputFormat<WritableComparable, Writable> getInputFormatFromCache(Class inputFormatClass,
      Configuration conf) throws IOException {
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance = (InputFormat<WritableComparable, Writable>) ReflectionUtils
            .newInstance(inputFormatClass, conf);
        inputFormats.put(inputFormatClass, newInstance);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!", e);
      }
    }
    return inputFormats.get(inputFormatClass);
  }

  private void setPrtnDesc() throws Exception {
    List<String> partNames = new ArrayList<String>();
    List<String> partValues = new ArrayList<String>();

    String pcols = currPart.getTableDesc().getProperties().getProperty(
        org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    LinkedHashMap<String, String> partSpec = currPart.getPartSpec();

    List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
    String[] partKeys = pcols.trim().split("/");
    for (String key : partKeys) {
      partNames.add(key);
      partValues.add(partSpec.get(key));
      partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    StructObjectInspector partObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(partNames, partObjectInspectors);
    StructObjectInspector noPartObjectInspector =
        (StructObjectInspector) serde.getObjectInspector();

    row[1] = partValues;
    rowObjectInspector = ObjectInspectorFactory.repackUnionStructObjectInspector(
        (UnionStructObjectInspector) rowObjectInspector,
        hasVC ? Arrays.asList(noPartObjectInspector, partObjectInspector, vcsOI) :
            Arrays.asList(noPartObjectInspector, partObjectInspector));
  }

  private void getNextPath() throws Exception {
    // first time
    if (iterPath == null) {
      if (work.getTblDir() != null) {
        if (!tblDataDone) {
          currPath = work.getTblDirPath();
          currTbl = work.getTblDesc();
          if (isNativeTable) {
            FileSystem fs = currPath.getFileSystem(job);
            if (fs.exists(currPath)) {
              FileStatus[] fStats = listStatusUnderPath(fs, currPath);
              for (FileStatus fStat : fStats) {
                if (fStat.getLen() > 0) {
                  tblDataDone = true;
                  break;
                }
              }
            }
          } else {
            tblDataDone = true;
          }

          if (!tblDataDone) {
            currPath = null;
          }
          return;
        } else {
          currTbl = null;
          currPath = null;
        }
        return;
      } else {
        iterPath = FetchWork.convertStringToPathArray(work.getPartDir()).iterator();
        iterPartDesc = work.getPartDesc().iterator();
      }
    }

    while (iterPath.hasNext()) {
      Path nxt = iterPath.next();
      PartitionDesc prt = null;
      if (iterPartDesc != null) {
        prt = iterPartDesc.next();
      }
      FileSystem fs = nxt.getFileSystem(job);
      if (fs.exists(nxt)) {
        FileStatus[] fStats = listStatusUnderPath(fs, nxt);
        for (FileStatus fStat : fStats) {
          if (fStat.getLen() > 0) {
            currPath = nxt;
            if (iterPartDesc != null) {
              currPart = prt;
            }
            return;
          }
        }
      }
    }
  }

  private RecordReader<WritableComparable, Writable> getRecordReader() throws Exception {
    if (currPath == null) {
      getNextPath();
      if (currPath == null) {
        return null;
      }

      // not using FileInputFormat.setInputPaths() here because it forces a
      // connection
      // to the default file system - which may or may not be online during pure
      // metadata
      // operations
      job.set("mapred.input.dir", org.apache.hadoop.util.StringUtils.escapeString(currPath
          .toString()));

      PartitionDesc tmp;
      if (currTbl == null) {
        tmp = currPart;
      } else {
        tmp = new PartitionDesc(currTbl, null);
      }

      Class<? extends InputFormat> formatter = tmp.getInputFileFormatClass();
      inputFormat = getInputFormatFromCache(formatter, job);
      Utilities.copyTableJobPropertiesToConf(tmp.getTableDesc(), job);
      InputSplit[] splits = inputFormat.getSplits(job, 1);
      inputSplits = new HiveInputFormat.HiveInputSplit[splits.length];
      for (int i = 0; i < splits.length; i++) {
        inputSplits[i] = new HiveInputFormat.HiveInputSplit(splits[i], formatter.getName());
      }
      splitNum = 0;
      serde = tmp.getDeserializerClass().newInstance();
      serde.initialize(job, tmp.getProperties());

      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating fetchTask with deserializer typeinfo: "
            + serde.getObjectInspector().getTypeName());
        LOG.debug("deserializer properties: " + tmp.getProperties());
      }

      if (currPart != null) {
        setPrtnDesc();
      }
    }

    if (splitNum >= inputSplits.length) {
      if (currRecReader != null) {
        currRecReader.close();
        currRecReader = null;
      }
      currPath = null;
      return getRecordReader();
    }

    @SuppressWarnings("unchecked")
    final RecordReader<WritableComparable, Writable> reader =
        inputFormat.getRecordReader(inputSplits[splitNum].getInputSplit(), job, Reporter.NULL);
    if (hasVC) {
      currRecReader = new HiveContextAwareRecordReader<WritableComparable, Writable>(reader, job) {
        public void doClose() throws IOException {
          reader.close();
        }
        public WritableComparable createKey() {
          return reader.createKey();
        }
        public Writable createValue() {
          return reader.createValue();
        }
        public long getPos() throws IOException {
          return reader.getPos();
        }
      };
      ((HiveContextAwareRecordReader)currRecReader).
          initIOContext(inputSplits[splitNum], job, inputFormat.getClass(), reader);
    } else {
      currRecReader = reader;
    }
    splitNum++;
    key = currRecReader.createKey();
    value = currRecReader.createValue();
    return currRecReader;
  }

  /**
   * Get the next row. The fetch context is modified appropriately.
   *
   **/
  public InspectableObject getNextRow() throws IOException {
    try {
      while (true) {
        if (context != null) {
          context.resetRow();
        }
        if (currRecReader == null) {
          currRecReader = getRecordReader();
          if (currRecReader == null) {
            return null;
          }
        }

        boolean ret = currRecReader.next(key, value);
        if (ret) {
          if (context != null && context.inputFileChanged()) {
            // The child operators cleanup if input file has changed
            try {
              ts.cleanUpInputFileChanged();
            } catch (HiveException e) {
              throw new IOException(e);
            }
          }
          if (hasVC) {
            List<VirtualColumn> vcs = ts.getConf().getVirtualCols();
            context.populateVirtualColumnValues(vcs, vcValues, serde);
            row[isPartitioned ? 2 : 1] = vcValues;
          }
          row[0] = serde.deserialize(value);
          if (hasVC || isPartitioned) {
            return new InspectableObject(row, rowObjectInspector);
          }
          return new InspectableObject(row[0], serde.getObjectInspector());
        } else {
          currRecReader.close();
          currRecReader = null;
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Clear the context, if anything needs to be done.
   *
   **/
  public void clearFetchContext() throws HiveException {
    try {
      if (currRecReader != null) {
        currRecReader.close();
        currRecReader = null;
      }
      if (context != null) {
        context.clear();
        context = null;
      }
      this.currPath = null;
      this.iterPath = null;
      this.iterPartDesc = null;
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  /**
   * used for bucket map join. there is a hack for getting partitionDesc. bucket map join right now
   * only allow one partition present in bucket map join.
   */
  public void setupContext(Iterator<Path> iterPath, Iterator<PartitionDesc> iterPartDesc) {
    this.iterPath = iterPath;
    this.iterPartDesc = iterPartDesc;
    if (iterPartDesc == null) {
      if (work.getTblDir() != null) {
        this.currTbl = work.getTblDesc();
      } else {
        // hack, get the first.
        List<PartitionDesc> listParts = work.getPartDesc();
        currPart = listParts.get(0);
      }
    }
  }

  public ObjectInspector getOutputObjectInspector() throws HiveException {
    try {
      if (work.getTblDir() != null) {
        TableDesc tbl = work.getTblDesc();
        Deserializer serde = tbl.getDeserializerClass().newInstance();
        serde.initialize(job, tbl.getProperties());
        StructObjectInspector oi = (StructObjectInspector) serde.getObjectInspector();
        if (hasVC) {
          return rowObjectInspector = ObjectInspectorFactory.repackUnionStructObjectInspector(
              (UnionStructObjectInspector) rowObjectInspector, Arrays.asList(oi, vcsOI));
        }
        return rowObjectInspector = oi;
      } else if (work.getPartDesc() != null) {
        List<PartitionDesc> listParts = work.getPartDesc();
        if(listParts.size() == 0) {
          return null;
        }
        currPart = listParts.get(0);
        serde = currPart.getTableDesc().getDeserializerClass().newInstance();
        serde.initialize(job, currPart.getTableDesc().getProperties());
        setPrtnDesc();
        currPart = null;
        return rowObjectInspector;
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  /**
   * Lists status for all files under a given path. Whether or not this is recursive depends on the
   * setting of job configuration parameter mapred.input.dir.recursive.
   *
   * @param fs
   *          file system
   *
   * @param p
   *          path in file system
   *
   * @return list of file status entries
   */
  private FileStatus[] listStatusUnderPath(FileSystem fs, Path p) throws IOException {
    HiveConf hiveConf = new HiveConf(job, FetchOperator.class);
    boolean recursive = hiveConf.getBoolVar(HiveConf.ConfVars.HADOOPMAPREDINPUTDIRRECURSIVE);
    if (!recursive) {
      return fs.listStatus(p);
    }
    List<FileStatus> results = new ArrayList<FileStatus>();
    for (FileStatus stat : fs.listStatus(p)) {
      FileUtils.listStatusRecursively(fs, stat, results);
    }
    return results.toArray(new FileStatus[results.size()]);
  }
}
