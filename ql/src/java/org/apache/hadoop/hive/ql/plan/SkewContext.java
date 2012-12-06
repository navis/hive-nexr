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

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Context for skewed RS
 */
public class SkewContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private ArrayList<ExprNodeDesc> skewKeys;
  private ArrayList<Integer> skewClusters;
  private ArrayList<Boolean> skewDrivers;

  private transient ExprNodeEvaluator[] keyEvals;
  private transient ObjectInspector[] keyObjectInspectors;

  private transient boolean[] drivers;
  private transient int[] clusters;

  public void initialize() throws HiveException {
    int i = 0;
    keyEvals = new ExprNodeEvaluator[skewKeys.size()];
    for (ExprNodeDesc e : skewKeys) {
      keyEvals[i++] = ExprNodeEvaluatorFactory.get(e);
    }
    drivers = getBooleanArray(skewDrivers);
    clusters = getIntegerArray(skewClusters);
  }

  public boolean initializeKey(HiveKey keyWritable,
      ObjectInspector rowInspector, int numReducer) throws HiveException {
    boolean runSkew = keyWritable.initializeSkew(numReducer, drivers, clusters);
    if (runSkew) {
      keyObjectInspectors = Operator.initEvaluators(keyEvals, rowInspector);
    }
    return runSkew;
  }

  private boolean[] getBooleanArray(List<Boolean> booleans) {
    boolean[] array = new boolean[booleans.size()];
    for ( int i = 0 ; i < array.length; i++) {
      array[i] = booleans.get(i);
    }
    return array;
  }

  private int[] getIntegerArray(List<Integer> integers) {
    int[] array = new int[integers.size()];
    for ( int i = 0 ; i < array.length; i++) {
      array[i] = integers.get(i);
    }
    return array;
  }

  @Explain(displayName = "key expressions")
  public ArrayList<ExprNodeDesc> getSkewKeys() {
    return skewKeys;
  }

  public void setSkewKeys(ArrayList<ExprNodeDesc> skewKeys) {
    this.skewKeys = skewKeys;
  }

  @Explain(displayName = "cluster sizes")
  public ArrayList<Integer> getSkewClusters() {
    return skewClusters;
  }

  public void setSkewClusters(ArrayList<Integer> skewClusters) {
    this.skewClusters = skewClusters;
  }

  @Explain(displayName = "drivers", normalExplain = false)
  public ArrayList<Boolean> getSkewDrivers() {
    return skewDrivers;
  }

  public void setSkewDrivers(ArrayList<Boolean> skewDrivers) {
    this.skewDrivers = skewDrivers;
  }

  // Evaluate skew group and hashcode for the row
  // If it's not skewed, return false for normal hashing
  public boolean evaluateSkew(Object row, HiveKey hiveKey) throws HiveException {
    for (int i = 0; i < keyEvals.length; i++) {
      Object eval = keyEvals[i].evaluate(row);
      if (eval != null && ((BooleanObjectInspector) keyObjectInspectors[i]).get(eval)) {
        hiveKey.setSkewGroup(i);
        return true;
      }
    }
    hiveKey.setSkewGroup(-1);
    return false;
  }

  public boolean isDriver(HiveKey hiveKey) {
    return drivers[hiveKey.getSkewGroup()];
  }
}
