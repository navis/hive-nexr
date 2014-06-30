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

package org.apache.hadoop.hive.ql.plan;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.ContentSummaryInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InputEstimator;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.BucketCol;
import org.apache.hadoop.hive.ql.optimizer.physical.BucketingSortingCtx.SortCol;
import org.apache.hadoop.hive.ql.parse.SplitSample;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.Interner;

/**
 * MapWork represents all the information used to run a map task on the cluster.
 * It is first used when the query planner breaks the logical plan into tasks and
 * used throughout physical optimization to track map-side operator plans, input
 * paths, aliases, etc.
 *
 * ExecDriver will serialize the contents of this class and make sure it is
 * distributed on the cluster. The ExecMapper will ultimately deserialize this
 * class on the data nodes and setup it's operator pipeline accordingly.
 *
 * This class is also used in the explain command any property with the
 * appropriate annotation will be displayed in the explain output.
 */
@SuppressWarnings({"serial", "deprecation"})
public class MapWork extends BaseWork {

  private static final Log LOG = LogFactory.getLog(MapWork.class);

  private boolean hadoopSupportsSplittable;

  // use LinkedHashMap to make sure the iteration order is
  // deterministic, to ease testing
  private LinkedHashMap<String, ArrayList<String>> pathToAliases = new LinkedHashMap<String, ArrayList<String>>();

  private LinkedHashMap<String, PartitionDesc> pathToPartitionInfo = new LinkedHashMap<String, PartitionDesc>();

  private LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();

  private LinkedHashMap<String, PartitionDesc> aliasToPartnInfo = new LinkedHashMap<String, PartitionDesc>();

  private HashMap<String, SplitSample> nameToSplitSample = new LinkedHashMap<String, SplitSample>();

  // If this map task has a FileSinkOperator, and bucketing/sorting metadata can be
  // inferred about the data being written by that operator, these are mappings from the directory
  // that operator writes into to the bucket/sort columns for that data.
  private final Map<String, List<BucketCol>> bucketedColsByDirectory =
      new HashMap<String, List<BucketCol>>();
  private final Map<String, List<SortCol>> sortedColsByDirectory =
      new HashMap<String, List<SortCol>>();

  private Path tmpHDFSPath;

  private String inputformat;

  private String indexIntermediateFile;

  private Integer numMapTasks;
  private Long maxSplitSize;
  private Long minSplitSize;
  private Long minSplitSizePerNode;
  private Long minSplitSizePerRack;

  //use sampled partitioning
  private int samplingType;

  public static final int SAMPLING_ON_PREV_MR = 1;  // todo HIVE-3841
  public static final int SAMPLING_ON_START = 2;    // sampling on task running

  // the following two are used for join processing
  private boolean leftInputJoin;
  private String[] baseSrc;
  private List<String> mapAliases;

  private boolean mapperCannotSpanPartns;

  // used to indicate the input is sorted, and so a BinarySearchRecordReader shoudl be used
  private boolean inputFormatSorted = false;

  private boolean useBucketizedHiveInputFormat;

  private boolean useOneNullRowInputFormat;

  private boolean dummyTableScan = false;

  // used for dynamic partitioning
  private Map<String, List<TableDesc>> eventSourceTableDescMap =
      new LinkedHashMap<String, List<TableDesc>>();
  private Map<String, List<String>> eventSourceColumnNameMap =
      new LinkedHashMap<String, List<String>>();
  private Map<String, List<ExprNodeDesc>> eventSourcePartKeyExprMap =
      new LinkedHashMap<String, List<ExprNodeDesc>>();

  private boolean doSplitsGrouping = true;
  private transient Path emptyScratchDir; // created if needed

  public MapWork() {}

  public MapWork(String name) {
    super(name);
  }

  @Explain(displayName = "Path -> Alias", normalExplain = false)
  public LinkedHashMap<String, ArrayList<String>> getPathToAliases() {
    return pathToAliases;
  }

  public void setPathToAliases(
      final LinkedHashMap<String, ArrayList<String>> pathToAliases) {
    this.pathToAliases = pathToAliases;
  }

  /**
   * This is used to display and verify output of "Path -> Alias" in test framework.
   *
   * QTestUtil masks "Path -> Alias" and makes verification impossible.
   * By keeping "Path -> Alias" intact and adding a new display name which is not
   * masked by QTestUtil by removing prefix.
   *
   * Notes: we would still be masking for intermediate directories.
   *
   * @return
   */
  @Explain(displayName = "Truncated Path -> Alias", normalExplain = false)
  public Map<String, ArrayList<String>> getTruncatedPathToAliases() {
    Map<String, ArrayList<String>> trunPathToAliases = new LinkedHashMap<String,
        ArrayList<String>>();
    Iterator<Entry<String, ArrayList<String>>> itr = this.pathToAliases.entrySet().iterator();
    while (itr.hasNext()) {
      final Entry<String, ArrayList<String>> entry = itr.next();
      String origiKey = entry.getKey();
      String newKey = PlanUtils.removePrefixFromWarehouseConfig(origiKey);
      ArrayList<String> value = entry.getValue();
      trunPathToAliases.put(newKey, value);
    }
    return trunPathToAliases;
  }

  @Explain(displayName = "Path -> Partition", normalExplain = false)
  public LinkedHashMap<String, PartitionDesc> getPathToPartitionInfo() {
    return pathToPartitionInfo;
  }

  public void setPathToPartitionInfo(
      final LinkedHashMap<String, PartitionDesc> pathToPartitionInfo) {
    this.pathToPartitionInfo = pathToPartitionInfo;
  }

  /**
   * Derive additional attributes to be rendered by EXPLAIN.
   * TODO: this method is relied upon by custom input formats to set jobconf properties.
   *       This is madness? - This is Hive Storage Handlers!
   */
  public void deriveExplainAttributes() {
    if (pathToPartitionInfo != null) {
      for (Map.Entry<String, PartitionDesc> entry : pathToPartitionInfo
          .entrySet()) {
        entry.getValue().deriveBaseFileName(entry.getKey());
      }
    }

    MapredLocalWork mapLocalWork = getMapRedLocalWork();
    if (mapLocalWork != null) {
      mapLocalWork.deriveExplainAttributes();
    }
  }

  public void internTable(Interner<TableDesc> interner) {
    if (aliasToPartnInfo != null) {
      for (PartitionDesc part : aliasToPartnInfo.values()) {
        if (part == null) {
          continue;
        }
        part.intern(interner);
      }
    }
    if (pathToPartitionInfo != null) {
      for (PartitionDesc part : pathToPartitionInfo.values()) {
        part.intern(interner);
      }
    }
  }

  /**
   * @return the aliasToPartnInfo
   */
  public LinkedHashMap<String, PartitionDesc> getAliasToPartnInfo() {
    return aliasToPartnInfo;
  }

  /**
   * @param aliasToPartnInfo
   *          the aliasToPartnInfo to set
   */
  public void setAliasToPartnInfo(
      LinkedHashMap<String, PartitionDesc> aliasToPartnInfo) {
    this.aliasToPartnInfo = aliasToPartnInfo;
  }

  public LinkedHashMap<String, Operator<? extends OperatorDesc>> getAliasToWork() {
    return aliasToWork;
  }

  public void setAliasToWork(
      final LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork) {
    this.aliasToWork = aliasToWork;
  }

  @Explain(displayName = "Split Sample", normalExplain = false)
  public HashMap<String, SplitSample> getNameToSplitSample() {
    return nameToSplitSample;
  }

  public void setNameToSplitSample(HashMap<String, SplitSample> nameToSplitSample) {
    this.nameToSplitSample = nameToSplitSample;
  }

  public Integer getNumMapTasks() {
    return numMapTasks;
  }

  public void setNumMapTasks(Integer numMapTasks) {
    this.numMapTasks = numMapTasks;
  }

  @SuppressWarnings("nls")
  public void addMapWork(String path, String alias, Operator<?> work,
      PartitionDesc pd) {
    ArrayList<String> curAliases = pathToAliases.get(path);
    if (curAliases == null) {
      assert (pathToPartitionInfo.get(path) == null);
      curAliases = new ArrayList<String>();
      pathToAliases.put(path, curAliases);
      pathToPartitionInfo.put(path, pd);
    } else {
      assert (pathToPartitionInfo.get(path) != null);
    }

    for (String oneAlias : curAliases) {
      if (oneAlias.equals(alias)) {
        throw new RuntimeException("Multiple aliases named: " + alias
            + " for path: " + path);
      }
    }
    curAliases.add(alias);

    if (aliasToWork.get(alias) != null) {
      throw new RuntimeException("Existing work for alias: " + alias);
    }
    aliasToWork.put(alias, work);
  }

  public boolean isInputFormatSorted() {
    return inputFormatSorted;
  }

  public void setInputFormatSorted(boolean inputFormatSorted) {
    this.inputFormatSorted = inputFormatSorted;
  }

  public void resolveDynamicPartitionStoredAsSubDirsMerge(HiveConf conf, Path path,
      TableDesc tblDesc, ArrayList<String> aliases, PartitionDesc partDesc) {
    pathToAliases.put(path.toString(), aliases);
    pathToPartitionInfo.put(path.toString(), partDesc);
  }

  /**
   * For each map side operator - stores the alias the operator is working on
   * behalf of in the operator runtime state. This is used by reduce sink
   * operator - but could be useful for debugging as well.
   */
  private void setAliases() {
    if(aliasToWork == null) {
      return;
    }
    for (String oneAlias : aliasToWork.keySet()) {
      aliasToWork.get(oneAlias).setAlias(oneAlias);
    }
  }

  @Explain(displayName = "Execution mode")
  public String getVectorModeOn() {
    return vectorMode ? "vectorized" : null;
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
    LinkedHashMap<String, Operator<? extends OperatorDesc>> newAliasToWork =
        new LinkedHashMap<String, Operator<?  extends OperatorDesc>>();

    for (Map.Entry<String, Operator<?>> entry: aliasToWork.entrySet()) {
      newAliasToWork.put(entry.getKey(), replacementMap.get(entry.getValue()));
    }

    setAliasToWork(newAliasToWork);
  }

  @Override
  @Explain(displayName = "Map Operator Tree")
  public Set<Operator<?>> getAllRootOperators() {
    Set<Operator<?>> opSet = new LinkedHashSet<Operator<?>>();

    Map<String, ArrayList<String>> pa = getPathToAliases();
    if (pa != null) {
      for (List<String> ls : pa.values()) {
        for (String a : ls) {
          Operator<?> op = getAliasToWork().get(a);
          if (op != null ) {
            opSet.add(op);
          }
        }
      }
    }
    return opSet;
  }

  public void mergeAliasedInput(String alias, String pathDir, PartitionDesc partitionInfo) {
    ArrayList<String> aliases = pathToAliases.get(pathDir);
    if (aliases == null) {
      aliases = new ArrayList<String>(Arrays.asList(alias));
      pathToAliases.put(pathDir, aliases);
      pathToPartitionInfo.put(pathDir, partitionInfo);
    } else {
      aliases.add(alias);
    }
  }

  public void initialize() {
    setAliases();
  }

  public Long getMaxSplitSize() {
    return maxSplitSize;
  }

  public void setMaxSplitSize(Long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  public Long getMinSplitSize() {
    return minSplitSize;
  }

  public void setMinSplitSize(Long minSplitSize) {
    this.minSplitSize = minSplitSize;
  }

  public Long getMinSplitSizePerNode() {
    return minSplitSizePerNode;
  }

  public void setMinSplitSizePerNode(Long minSplitSizePerNode) {
    this.minSplitSizePerNode = minSplitSizePerNode;
  }

  public Long getMinSplitSizePerRack() {
    return minSplitSizePerRack;
  }

  public void setMinSplitSizePerRack(Long minSplitSizePerRack) {
    this.minSplitSizePerRack = minSplitSizePerRack;
  }

  public String getInputformat() {
    return inputformat;
  }

  public void setInputformat(String inputformat) {
    this.inputformat = inputformat;
  }

  public boolean isUseBucketizedHiveInputFormat() {
    return useBucketizedHiveInputFormat;
  }

  public void setUseBucketizedHiveInputFormat(boolean useBucketizedHiveInputFormat) {
    this.useBucketizedHiveInputFormat = useBucketizedHiveInputFormat;
  }

  public void setUseOneNullRowInputFormat(boolean useOneNullRowInputFormat) {
    this.useOneNullRowInputFormat = useOneNullRowInputFormat;
  }

  public boolean isUseOneNullRowInputFormat() {
    return useOneNullRowInputFormat;
  }

  public void setMapperCannotSpanPartns(boolean mapperCannotSpanPartns) {
    this.mapperCannotSpanPartns = mapperCannotSpanPartns;
  }

  public boolean isMapperCannotSpanPartns() {
    return this.mapperCannotSpanPartns;
  }

  public boolean getHadoopSupportsSplittable() {
    return hadoopSupportsSplittable;
  }

  public void setHadoopSupportsSplittable(boolean hadoopSupportsSplittable) {
    this.hadoopSupportsSplittable = hadoopSupportsSplittable;
  }

  public String getIndexIntermediateFile() {
    return indexIntermediateFile;
  }

  public ArrayList<String> getAliases() {
    return new ArrayList<String>(aliasToWork.keySet());
  }

  public ArrayList<Operator<?>> getWorks() {
    return new ArrayList<Operator<?>>(aliasToWork.values());
  }

  public ArrayList<String> getPaths() {
    return new ArrayList<String>(pathToAliases.keySet());
  }

  public ArrayList<PartitionDesc> getPartitionDescs() {
    return new ArrayList<PartitionDesc>(aliasToPartnInfo.values());
  }

  public Path getTmpHDFSPath() {
    return tmpHDFSPath;
  }

  public void setTmpHDFSPath(Path tmpHDFSPath) {
    this.tmpHDFSPath = tmpHDFSPath;
  }

  public void mergingInto(MapWork mapWork) {
    // currently, this is sole field affecting mergee task
    mapWork.useBucketizedHiveInputFormat |= useBucketizedHiveInputFormat;
  }

  @Explain(displayName = "Path -> Bucketed Columns", normalExplain = false)
  public Map<String, List<BucketCol>> getBucketedColsByDirectory() {
    return bucketedColsByDirectory;
  }

  @Explain(displayName = "Path -> Sorted Columns", normalExplain = false)
  public Map<String, List<SortCol>> getSortedColsByDirectory() {
    return sortedColsByDirectory;
  }

  public void addIndexIntermediateFile(String fileName) {
    if (this.indexIntermediateFile == null) {
      this.indexIntermediateFile = fileName;
    } else {
      this.indexIntermediateFile += "," + fileName;
    }
  }

  public int getSamplingType() {
    return samplingType;
  }

  public void setSamplingType(int samplingType) {
    this.samplingType = samplingType;
  }

  @Explain(displayName = "Sampling", normalExplain = false)
  public String getSamplingTypeString() {
    return samplingType == 1 ? "SAMPLING_ON_PREV_MR" :
        samplingType == 2 ? "SAMPLING_ON_START" : null;
  }

  @Override
  public void configureJobConf(JobConf job) {
    for (PartitionDesc partition : aliasToPartnInfo.values()) {
      PlanUtils.configureJobConf(partition.getTableDesc(), job);
    }
    Collection<Operator<?>> mappers = aliasToWork.values();
    for (FileSinkOperator fs : OperatorUtils.findOperators(mappers, FileSinkOperator.class)) {
      PlanUtils.configureJobConf(fs.getConf().getTableInfo(), job);
    }
  }

  public void logPathToAliases() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("LOGGING PATH TO ALIASES");
      for (Map.Entry<String, ArrayList<String>> entry: pathToAliases.entrySet()) {
        for (String a: entry.getValue()) {
          LOG.debug("Path: " + entry.getKey() + ", Alias: " + a);
        }
      }
    }
  }


  public void setDummyTableScan(boolean dummyTableScan) {
    this.dummyTableScan = dummyTableScan;
  }

  public boolean getDummyTableScan() {
    return dummyTableScan;
  }

  public void setEventSourceTableDescMap(Map<String, List<TableDesc>> map) {
    this.eventSourceTableDescMap = map;
  }

  public Map<String, List<TableDesc>> getEventSourceTableDescMap() {
    return eventSourceTableDescMap;
  }

  public void setEventSourceColumnNameMap(Map<String, List<String>> map) {
    this.eventSourceColumnNameMap = map;
  }

  public Map<String, List<String>> getEventSourceColumnNameMap() {
    return eventSourceColumnNameMap;
  }

  public Map<String, List<ExprNodeDesc>> getEventSourcePartKeyExprMap() {
    return eventSourcePartKeyExprMap;
  }

  public void setEventSourcePartKeyExprMap(Map<String, List<ExprNodeDesc>> map) {
    this.eventSourcePartKeyExprMap = map;
  }

  public void setDoSplitsGrouping(boolean doSplitsGrouping) {
    this.doSplitsGrouping = doSplitsGrouping;
  }

  public boolean getDoSplitsGrouping() {
    return this.doSplitsGrouping;
  }

  public boolean isLeftInputJoin() {
    return leftInputJoin;
  }

  public void setLeftInputJoin(boolean leftInputJoin) {
    this.leftInputJoin = leftInputJoin;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public List<String> getMapAliases() {
    return mapAliases;
  }

  public void setMapAliases(List<String> mapAliases) {
    this.mapAliases = mapAliases;
  }

  public Path getEmptyScratchDir(Context ctx) throws IOException {
    if (emptyScratchDir == null) {
      emptyScratchDir = ctx.createMRTmpPath();
    }
    return emptyScratchDir;
  }

  private static final ObjectInspector VC_FILE_OI =
      ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.asList(VirtualColumn.FILENAME.getName()),
        Arrays.asList((ObjectInspector) PrimitiveObjectInspectorFactory.javaStringObjectInspector));

  private static final BooleanObjectInspector EVAL_OI =
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;

  private transient Map<String, ExprNodeEvaluator> pathToEvals;
  private transient Map<String, InputSummary> summaries;

  public ContentSummary getSummaryFor(String inputPath, Configuration conf)
      throws IOException, HiveException {
    return getInputSummaryFor(inputPath, conf).toContentSummary();
  }

  public void resetIntermediary(Context ctx) {
    if (summaries == null) {
      return;
    }
    List<String> invalidates = new ArrayList<String>();
    for (String path : summaries.keySet()) {
      if (ctx.isMRTmpFileURI(path)) {
        invalidates.add(path);
      }
    }
    for (String invalidate : invalidates) {
      summaries.remove(invalidate);
    }
  }

  public boolean pathExists(String inputPath, Configuration conf)
      throws IOException, HiveException {
    InputSummary summary = getInputSummaryFor(inputPath, conf);
    if (summary.exists && (
        summary.fileCount > 0 || summary.length > 0 || summary.directoryCount > 1)) {
      return true;  // for MR, some valid file should exit in it
    }
    return false;
  }

  public void replaceToDummy(String inputPath, Path dummyPath, Configuration conf)
      throws IOException, HiveException {
    summaries.remove(inputPath);
    pathToEvals.remove(inputPath);  // no need to evaluation for dummy path
    summaries.put(dummyPath.toString(), new InputSummary(dummyPath));
  }

  // if file pruning is applied, return paths passed the filter. if not, return input path
  public List<Path> getPrunedPaths(String inputPath, Configuration conf)
      throws IOException, HiveException {
    return getInputSummaryFor(inputPath, conf).paths;
  }

  private InputSummary getInputSummaryFor(String inputPath, Configuration conf)
      throws IOException, HiveException {
    InputSummary summary = summaries == null ? null : summaries.get(inputPath);
    if (summary == null) {
      ExprNodeEvaluator evaluator = getPathToEvals().get(inputPath);
      if (summaries == null) {
        summaries = new HashMap<String, InputSummary>();
      }
      summaries.put(inputPath, summary = summarize(inputPath, conf, evaluator));
    }
    return summary;
  }

  // get summaries for all input paths and return total of them
  public ContentSummary getTotalSummary(Context ctx) throws IOException, HiveException {
    long length = 0;
    long fileCount = 0;
    long directoryCount = 0;
    for (String path : pathToAliases.keySet()) {
      ContentSummary pathSummary = getSummaryFor(path, ctx.getConf());
      if (pathSummary != null) {
        if (pathSummary.getLength() > 0) {
          length += pathSummary.getLength();
        }
        if (pathSummary.getFileCount() > 0) {
          fileCount += pathSummary.getFileCount();
        }
        if (pathSummary.getDirectoryCount() > 0) {
          directoryCount += pathSummary.getDirectoryCount();
        }
        ctx.addCS(path, pathSummary);
      }
    }
    return new ContentSummary(length, fileCount, directoryCount);
  }

  // return or-conjuncted file pruning filter
  private ExprNodeEvaluator toFilter(List<Operator<?>> operators) throws HiveException {
    ExprNodeDesc prev = null;
    for (Operator<?> operator : operators) {
      if (operator instanceof TableScanOperator && operator.getConf() != null) {
        ExprNodeDesc filterExpr = ((TableScanOperator) operator).getConf().getFileFilterExpr();
        if (filterExpr == null) {
          continue;
        }
        if (prev == null) {
          prev = filterExpr;
        } else {
          prev = ExprNodeDescUtils.orPredicates(prev, filterExpr);
        }
      }
    }
    if (prev == null) {
      return null;
    }
    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(prev);
    evaluator.initialize(VC_FILE_OI);
    return evaluator;
  }

  // evaluate input path with file pruning filter
  private InputSummary summarize(String pathStr, Configuration conf, ExprNodeEvaluator evaluator)
      throws IOException, HiveException {
    Path path = new Path(pathStr);
    PartitionDesc partDesc = pathToPartitionInfo.get(pathStr);
    Class<? extends InputFormat> format = partDesc.getInputFileFormatClass();
    if (ContentSummaryInputFormat.class.isAssignableFrom(format)) {
      JobConf jobConf = new JobConf(conf);
      ContentSummaryInputFormat summaryInput = (ContentSummaryInputFormat)
          HiveInputFormat.getInputFormatFromCache(format, jobConf);
      ContentSummary content = summaryInput.getContentSummary(path, jobConf);
      return new InputSummary(path, content);
    }
    HiveStorageHandler handler = HiveUtils.getStorageHandler(conf,
        SerDeUtils.createOverlayedProperties(
            partDesc.getTableDesc().getProperties(),
            partDesc.getProperties()).getProperty(hive_metastoreConstants.META_TABLE_STORAGE));
    if (handler instanceof InputEstimator) {
      long total = 0;
      TableDesc tableDesc = partDesc.getTableDesc();
      InputEstimator estimator = (InputEstimator) handler;
      for (String alias : HiveFileFormatUtils.doGetAliasesFromPath(pathToAliases, path)) {
        JobConf jobConf = new JobConf(conf);
        TableScanOperator scanOp = (TableScanOperator) aliasToWork.get(alias);
        Utilities.setColumnNameList(jobConf, scanOp, true);
        Utilities.setColumnTypeList(jobConf, scanOp, true);
        PlanUtils.configureInputJobPropertiesForStorageHandler(tableDesc);
        Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf);
        total += estimator.estimate(jobConf, scanOp, -1).getTotalLength();
      }
      return new InputSummary(path, new ContentSummary(total, -1, -1));
    }
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return new InputSummary(false);
    }
    if (evaluator == null) {
      ContentSummary summary = fs.getContentSummary(path);
      return new InputSummary(path, summary);
    }
    InputSummary summary = new InputSummary(true);
    for (FileStatus inputStatus : getInputStatus(path, conf)) {
      String relative = relativize(path, inputStatus.getPath());
      Object evaluated = evaluator.evaluate(new String[]{relative});
      if (EVAL_OI.get(evaluated)) {
        summary.add(new Path(path, relative), inputStatus);
      }
    }
    return summary;
  }

  private String relativize(Path inputPath, Path childPath) {
    URI relativized = inputPath.toUri().relativize(childPath.toUri());
    if (relativized == childPath.toUri()) {
      throw new IllegalStateException("Invalid child path " + childPath + " for input " + inputPath);
    }
    return relativized.getPath();
  }

  // todo exploit path filter
  private FileStatus[] getInputStatus(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status;
    try {
      status = fs.getFileStatus(path);
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
    if (status.isDir()) {
      return fs.globStatus(new Path(path, "/*"));
    }
    return new FileStatus[]{status};
  }

  private Map<String, ExprNodeEvaluator> getPathToEvals() throws HiveException {
    if (pathToEvals == null) {
      pathToEvals = new HashMap<String, ExprNodeEvaluator>();
    }
    for (Map.Entry<String, ArrayList<String>> entry : pathToAliases.entrySet()) {
      pathToEvals.put(entry.getKey(), toFilter(getWorkForAliases(entry.getValue())));
    }
    return pathToEvals;
  }

  private List<Operator<?>> getWorkForAliases(List<String> aliases) {
    List<Operator<?>> operators = new ArrayList<Operator<?>>();
    for (String alias : aliases) {
      Operator<? extends OperatorDesc> work = aliasToWork.get(alias);
      if (work == null) {
        throw new IllegalStateException("Invalid alias " + alias);
      }
      operators.add(work);
    }
    return operators;
  }

  private static class InputSummary {

    private boolean exists;

    private long length;
    private long fileCount;
    private long directoryCount;
    private List<Path> paths;

    public InputSummary(boolean exists) {
      paths = exists ? new ArrayList<Path>() : Collections.<Path>emptyList();
    }

    public InputSummary(Path path) {
      paths = Arrays.asList(path);
      exists = true;
    }

    public InputSummary(Path path, ContentSummary content) {
      this.paths = Arrays.asList(path);
      this.length = content.getLength();
      this.fileCount = content.getFileCount();
      this.directoryCount = content.getDirectoryCount();
      this.exists = true;
    }

    public void add(Path path, FileStatus status) {
      paths.add(path);
      length += status.getLen();
      if (!status.isDir()) {
        fileCount++;
      } else {
        directoryCount++;
      }
      exists = true;
    }

    public ContentSummary toContentSummary() {
      return exists ? new ContentSummary(length, fileCount, directoryCount) : null;
    }
  }
}
