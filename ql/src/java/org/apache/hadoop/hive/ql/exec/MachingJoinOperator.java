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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.serde2.SerDeException;

/**
 * Map side Join operator implementation.
 */
public abstract class MachingJoinOperator<T extends MapJoinDesc> extends AbstractMapJoinOperator<T> implements Serializable {

  protected static final long serialVersionUID = 1L;

  protected static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join exceeds available memory. "
          + "Please try removing the mapjoin hint."};

  protected transient Map<Byte, MapJoinRowContainer<ArrayList<Object>>> rowContainerMap;
  protected transient int metadataKeyTag;
  protected transient int[] metadataValueTag;
  protected transient boolean hashTblInitedOnce;

  protected int bigTableAlias;

  public MachingJoinOperator() {
  }

  public MachingJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    super.initializeOp(hconf);

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }

    metadataKeyTag = -1;
    bigTableAlias = order[posBigTable];

    rowContainerMap = new HashMap<Byte, MapJoinRowContainer<ArrayList<Object>>>();
    // initialize the hash tables for other tables
    for (int pos = 0; pos < numAliases; pos++) {
      if (pos == posBigTable) {
        continue;
      }
      MapJoinRowContainer<ArrayList<Object>> rowContainer = new MapJoinRowContainer<ArrayList<Object>>();
      rowContainerMap.put(Byte.valueOf((byte) pos), rowContainer);
    }

    hashTblInitedOnce = false;
  }

  @Override
  protected void fatalErrorMessage(StringBuilder errMsg, long counterCode) {
    errMsg.append("Operator ").append(getOperatorId());
    errMsg.append(" (id=").append(id).append("): ").append(FATAL_ERR_MSG[(int) counterCode]);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    try {
      if (firstRow) {
        handleFirstRow();
        firstRow = false;
      }

      // get alias
      alias = order[tag];
      // alias = (byte)tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias))) {
        nextSz = joinEmitInterval;
      }

      // compute keys and values as StandardObjects
      AbstractMapJoinKey key = JoinUtil.computeMapJoinKeys(row, joinKeys.get(alias),
          joinKeysObjectInspectors.get(alias));
      ArrayList<Object> value = JoinUtil.computeValues(row, joinValues.get(alias),
          joinValuesObjectInspectors.get(alias), joinFilters.get(alias), joinFilterObjectInspectors
              .get(alias), filterMap == null ? null : filterMap[alias]);


      // Add the value to the ArrayList
      storage.get((byte) tag).add(value);

      for (Byte pos : order) {
        if (pos.intValue() != tag) {

          MapJoinObjectValue o = getValueFor(key, pos);
          MapJoinRowContainer<ArrayList<Object>> rowContainer = rowContainerMap.get(pos);

          // there is no join-value or join-key has all null elements
          if (o == null || key.hasAnyNulls(nullsafes)) {
            if (noOuterJoin) {
              storage.put(pos, emptyList);
            } else {
              storage.put(pos, dummyObjVectors[pos.intValue()]);
            }
          } else {
            rowContainer.reset(o.getObj());
            storage.put(pos, rowContainer);
          }
        }
      }

      // generate the output records
      checkAndGenObject();

      // done with the row
      storage.get((byte) tag).clear();

      for (Byte pos : order) {
        if (pos.intValue() != tag) {
          storage.put(pos, null);
        }
      }

    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  protected abstract void handleFirstRow() throws HiveException, SerDeException;

  protected abstract MapJoinObjectValue getValueFor(AbstractMapJoinKey key, Byte pos);
}
