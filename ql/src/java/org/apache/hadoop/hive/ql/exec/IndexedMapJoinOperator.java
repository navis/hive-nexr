package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexedMapJoinOperator extends AbstractMapJoinOperator<SMBJoinDesc>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final Log LOG = LogFactory.getLog(SMBMapJoinOperator.class.getName());

  private Map<String, Operator<? extends Serializable>> inputOperators = Collections.emptyMap();
  private Map<String, SearchOperator> fetchOperators = Collections.emptyMap();

  private transient ArrayList<Object> keyWritable;
  private transient ArrayList<Object> nextKeyWritable;
  private transient RowContainer<ArrayList<Object>>[] candidateStorage;

  private transient Map<Byte, String> tagToAlias;

  private transient boolean localWorkInited;
  private transient boolean closeCalled;

  public IndexedMapJoinOperator() {
  }

  public IndexedMapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mapJoinOp) {
    super(mapJoinOp);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    firstRow = true;
    closeCalled = false;

    // get the largest table alias from order
    int maxAlias = 0;
    for (Byte alias: order) {
      if (alias > maxAlias) {
        maxAlias = alias;
      }
    }
    maxAlias += 1;

    candidateStorage = new RowContainer[maxAlias];

    int bucketSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);

    byte storePos = (byte) 0;
    for (Byte alias : order) {
      candidateStorage[alias] = JoinUtil.getRowContainer(hconf,
          rowContainerStandardObjectInspectors.get(storePos),
          alias, bucketSize, spillTableDesc, conf, noOuterJoin);
      storePos++;
    }
    tagToAlias = conf.getTagToAlias();
  }

  @Override
  public void initializeLocalWork(Configuration hconf) throws HiveException {
    initializeMapredLocalWork(this.getConf(), hconf, this.getConf().getLocalWork(), LOG);
    super.initializeLocalWork(hconf);
  }

  public void initializeMapredLocalWork(MapJoinDesc conf, Configuration hconf,
      MapredLocalWork localWork, Log l4j) throws HiveException {
    if (localWork == null || localWorkInited) {
      return;
    }
    localWorkInited = true;

    inputOperators = localWork.getAliasToWork();
    fetchOperators = new HashMap<String, SearchOperator>();

    Map<SearchOperator, JobConf> searcherConfMap = new HashMap<SearchOperator, JobConf>();
    // create map local operators
    for (Map.Entry<String, FetchWork> entry : localWork.getAliasToFetchWork()
        .entrySet()) {
      JobConf jobClone = new JobConf(hconf);
      Operator<? extends Serializable> tableScan = localWork.getAliasToWork()
      .get(entry.getKey());
      if(tableScan instanceof TableScanOperator) {
        ArrayList<Integer> list = ((TableScanOperator)tableScan).getNeededColumnIDs();
        if (list != null) {
          ColumnProjectionUtils.appendReadColumnIDs(jobClone, list);
        }
      } else {
        ColumnProjectionUtils.setFullyReadColumns(jobClone);
      }
      SearchOperator searcher = new SearchOperator(entry.getValue(),jobClone);
      searcherConfMap.put(searcher, jobClone);
      fetchOperators.put(entry.getKey(), searcher);
      l4j.info("fetchoperator for " + entry.getKey() + " created");
    }

    for (Map.Entry<String, SearchOperator> entry : fetchOperators.entrySet()) {
      Operator<? extends Serializable> forwardOp = localWork.getAliasToWork()
          .get(entry.getKey());
      // All the operators need to be initialized before process
      forwardOp.setExecContext(this.getExecContext());
      SearchOperator searcher = entry.getValue();
      JobConf jobConf = searcherConfMap.get(searcher);
      if (jobConf == null) {
        jobConf = this.getExecContext().getJc();
      }
      forwardOp.initialize(jobConf, new ObjectInspector[] {searcher.getOutputObjectInspector()});
      l4j.info("fetchoperator for " + entry.getKey() + " initialized");
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    byte alias = (byte) tag;

    // compute keys and values as StandardObjects
    ArrayList<Object> key = JoinUtil.computeKeys(row, joinKeys.get(alias),
        joinKeysObjectInspectors.get(alias));
    ArrayList<Object> value = JoinUtil.computeValues(row, joinValues.get(alias),
        joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
        joinFilterObjectInspectors.get(alias), noOuterJoin);

    candidateStorage[tag].add(value);

    if (tag == posBigTable) {
      if (processKey(key)) {
        storage.put(alias, candidateStorage[tag]);
        joinObject();
      }
    }
    reportProgress();
    numMapRowsRead++;
  }

  private boolean processKey(ArrayList<Object> key) throws HiveException {
    if (keyWritable == null) {
      keyWritable = key;
      return false;
    }
    int cmp = compareKeys(key, keyWritable);
    if (cmp != 0) {
      nextKeyWritable = key;
      return true;
    }
    return false;
  }

  private int compareKeys(List<Object> k1, List<Object> k2) {
    // join keys have difference sizes?
    int ret = k1.size() - k2.size();
    if (ret != 0) {
      return ret;
    }
    for (int i = 0; i < k1.size(); i++) {
      WritableComparable key_1 = (WritableComparable) k1.get(i);
      WritableComparable key_2 = (WritableComparable) k2.get(i);
      if (key_1 == null && key_2 == null) {
        return nullsafes != null && nullsafes[i] ? 0 : -1; // just return k1 is smaller than k2
      } else if (key_1 == null) {
        return -1;
      } else if (key_2 == null) {
        return 1;
      }
      ret = WritableComparator.get(key_1.getClass()).compare(key_1, key_2);
      if(ret != 0) {
        return ret;
      }
    }
    return ret;
  }

  private void joinObject() throws HiveException {
    for (Byte tag : order) {
      if (tag == posBigTable) {
        continue;
      }
      String tble = tagToAlias.get(tag);
      SearchOperator searcher = fetchOperators.get(tble);
      Iterable<Object> result = searcher.search(tag, keyWritable);
      if (result == null) {
        putDummyOrEmpty(tag);
      } else {
        Operator<?> operator = inputOperators.get(tble);
        for (Object value : result) {
          operator.process(value, 0);
        }
      }
      storage.put(tag, candidateStorage[tag]);
    }

    checkAndGenObject();

    for (Byte alias : order) {
      candidateStorage[alias].clear();
    }
    keyWritable = nextKeyWritable;
    nextKeyWritable = null;
  }

  private void putDummyOrEmpty(Byte i) {
    // put a empty list or null
    if (noOuterJoin) {
      storage.put(i, emptyList);
    } else {
      storage.put(i, dummyObjVectors[i.intValue()]);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if(closeCalled) {
      return;
    }
    closeCalled = true;
    localWorkInited = false;

    super.closeOp(abort);

    for (Map.Entry<String, SearchOperator> entry : fetchOperators.entrySet()) {
      Operator<? extends Serializable> searcher = inputOperators.get(entry.getKey());
      if (searcher != null) {
        searcher.close(abort);
      }
    }
  }

  @Override
  protected boolean allInitializedParentsAreClosed() {
    return true;
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return "MAPJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }
}
