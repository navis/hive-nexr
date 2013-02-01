/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.PriorityQueue;

/**
 * Merges partially sorted multiple streams into single one.
 * Before fetching, setupSegments() should be called (can be reused by calling it again).
 *
 * setupFetchContext() makes OIs and evals for keys, which is needed for sorting.
 * But for SMBJoin, it's already prepared in operator and uses it instead.
 */
public abstract class MergeSortingFetcher extends PriorityQueue<Integer>
    implements RowFetcher, Comparator<List<Object>> {

  protected final FetchWork fetchWork;
  protected final JobConf jobConf;

  // for keeping track of the number of elements read. just for debugging
  protected transient int counter;

  protected transient FetchOperator[] segments;
  protected transient List<ExprNodeEvaluator> keyFields;
  protected transient List<ObjectInspector> keyFieldOIs;

  // index of FetchOperator which is providing smallest one
  protected transient Integer currentMinSegment;
  protected transient ObjectPair<List<Object>, InspectableObject>[] keys;

  public MergeSortingFetcher(FetchWork fetchWork, JobConf jobConf) {
    this.fetchWork = fetchWork;
    this.jobConf = jobConf;
  }

  public ObjectInspector setupFetchContext() throws HiveException {
    TableDesc table = fetchWork.getTblDesc();
    ObjectInspector rowOI = getObjectInspector(table, jobConf);
    List<ExprNodeDesc> exprs = fetchWork.getMergeKeys();
    keyFieldOIs = new ArrayList<ObjectInspector>(exprs.size());
    keyFields = new ArrayList<ExprNodeEvaluator>(exprs.size());
    for (ExprNodeDesc expr : exprs) {
      ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(expr);
      keyFieldOIs.add(evaluator.initialize(rowOI));
      keyFields.add(evaluator);
    }
    return rowOI;
  }

  protected List<Path> getPaths(Path tblPath, JobConf conf) throws HiveException {
    try {
      FileSystem fs = tblPath.getFileSystem(conf);
      List<Path> paths = new ArrayList<Path>();
      for (FileStatus status : FetchOperator.listStatusUnderPath(fs, tblPath, conf)) {
        if (!status.isDir() && status.getLen() > 0) {
          paths.add(status.getPath());
        }
      }
      return paths;
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  protected ObjectInspector getObjectInspector(TableDesc table, JobConf conf)
    throws HiveException {
    try {
      Deserializer serde = table.getDeserializerClass().newInstance();
      serde.initialize(conf, table.getProperties());
      return serde.getObjectInspector();
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  // Setup FetchOperators for reading. This should be called just before row fetching
  protected void setupSegments(List<Path> paths) throws HiveException {
    int segmentLen = paths.size();
    FetchOperator[] segments = segmentsForSize(segmentLen);
    for (int i = 0; i < segmentLen; i++) {
      Path path = paths.get(i);
      if (segments[i] == null) {
        segments[i] = new FetchOperator(fetchWork, new JobConf(jobConf));
      }
      segments[i].setupContext(Arrays.asList(path));
    }
    initialize(segmentLen);
    for (int i = 0; i < segmentLen; i++) {
      if (nextHive(i)) {
        put(i);
      }
    }
    counter = 0;
  }

  @SuppressWarnings("unchecked")
  private FetchOperator[] segmentsForSize(int segmentLen) {
    if (segments == null || segments.length < segmentLen) {
      FetchOperator[] newSegments = new FetchOperator[segmentLen];
      ObjectPair<List<Object>, InspectableObject>[] newKeys = new ObjectPair[segmentLen];
      if (segments != null) {
        System.arraycopy(segments, 0, newSegments, 0, segments.length);
        System.arraycopy(keys, 0, newKeys, 0, keys.length);
      }
      segments = newSegments;
      keys = newKeys;
    }
    return segments;
  }

  public void clearFetchContext() throws HiveException {
    if (segments != null) {
      for (FetchOperator op : segments) {
        if (op != null) {
          op.clearFetchContext();
        }
      }
    }
  }

  protected final boolean lessThan(Object a, Object b) {
    return compare(keys[(Integer) a].getFirst(), keys[(Integer) b].getFirst()) < 0;
  }

  public InspectableObject fetchRow() throws IOException, HiveException {
    if (currentMinSegment != null) {
      adjustPriorityQueue(currentMinSegment);
    }
    Integer current = top();
    if (current == null) {
      return null;
    }
    counter++;
    return keys[currentMinSegment = current].getSecond();
  }

  private void adjustPriorityQueue(int current) throws IOException {
    if (nextIO(current)) {
      adjustTop();  // sort
    } else {
      pop();
    }
  }

  // wrapping for exception handling
  private boolean nextHive(int current) throws HiveException {
    try {
      return next(current);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  // wrapping for exception handling
  private boolean nextIO(int current) throws IOException {
    try {
      return next(current);
    } catch (HiveException e) {
      throw new IOException(e);
    }
  }

  // return true if current min segment(FetchOperator) has next row
  protected boolean next(int current) throws IOException, HiveException {
    InspectableObject nextRow = readRow(current);
    for (; nextRow != null; nextRow = readRow(current)) {
      if (keys[current] == null) {
        keys[current] = new ObjectPair<List<Object>, InspectableObject>();
      }
      // It is possible that the row got absorbed in the operator tree.
      if (nextRow.o != null) {
        // todo this should be changed to be evaluated lazily, especially for single segment case
        keys[current].setFirst(JoinUtil.computeKeys(nextRow.o, keyFields, keyFieldOIs));
        keys[current].setSecond(nextRow);
        return true;
      }
    }
    keys[current] = null;
    return false;
  }

  protected InspectableObject readRow(int current) throws IOException, HiveException {
    return readFromSegment(current);
  }

  protected final InspectableObject readFromSegment(int current) throws IOException {
    return segments[current].fetchRow();
  }
}
