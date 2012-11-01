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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.ReflectionUtils;

public class MapJoinOperator extends MachingJoinOperator<MapJoinDesc> {

  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());

  private transient Map<Byte, HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>> mapJoinTables;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    mapJoinTables = new HashMap<Byte, HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>>();
    for (int pos = 0; pos < numAliases; pos++) {
      if (pos != posBigTable) {
        mapJoinTables.put(Byte.valueOf((byte) pos),
            new HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>());
      }
    }
  }

  private void generateMapMetaData() throws HiveException, SerDeException {
    // generate the meta data for key
    // index for key is -1
    TableDesc keyTableDesc = conf.getKeyTblDesc();
    SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(),
        null);
    keySerializer.initialize(null, keyTableDesc.getProperties());
    MapJoinMetaData.put(Integer.valueOf(metadataKeyTag), new HashTableSinkOperator.HashTableSinkObjectCtx(
        ObjectInspectorUtils.getStandardObjectInspector(keySerializer.getObjectInspector(),
            ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE), keySerializer, keyTableDesc, hconf));

    // index for values is just alias
    for (int tag = 0; tag < order.length; tag++) {
      int alias = (int) order[tag];

      if (alias == this.bigTableAlias) {
        continue;
      }


      TableDesc valueTableDesc;
      if (conf.getNoOuterJoin()) {
        valueTableDesc = conf.getValueTblDescs().get(tag);
      } else {
        valueTableDesc = conf.getValueFilteredTblDescs().get(tag);
      }
      SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(),
          null);
      valueSerDe.initialize(null, valueTableDesc.getProperties());

      MapJoinMetaData.put(Integer.valueOf(alias), new HashTableSinkOperator.HashTableSinkObjectCtx(ObjectInspectorUtils
          .getStandardObjectInspector(valueSerDe.getObjectInspector(),
              ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE), valueSerDe, valueTableDesc, hconf));
    }
  }

  private void loadHashTable() throws HiveException {

    if (!this.getExecContext().getLocalWork().getInputFileChangeSensitive()) {
      if (hashTblInitedOnce) {
        return;
      } else {
        hashTblInitedOnce = true;
      }
    }

    String baseDir = null;

    String currentInputFile = getExecContext().getCurrentInputFile();
    LOG.info("******* Load from HashTable File: input : " + currentInputFile);

    String fileName = getExecContext().getLocalWork().getBucketFileName(currentInputFile);

    try {
      if (ShimLoader.getHadoopShims().isLocalMode(hconf)) {
        baseDir = this.getExecContext().getLocalWork().getTmpFileURI();
      } else {
        Path[] localArchives;
        String stageID = this.getExecContext().getLocalWork().getStageID();
        String suffix = Utilities.generateTarFileName(stageID);
        FileSystem localFs = FileSystem.getLocal(hconf);
        localArchives = DistributedCache.getLocalCacheArchives(this.hconf);
        Path archive;
        for (int j = 0; j < localArchives.length; j++) {
          archive = localArchives[j];
          if (!archive.getName().endsWith(suffix)) {
            continue;
          }
          Path archiveLocalLink = archive.makeQualified(localFs);
          baseDir = archiveLocalLink.toUri().getPath();
        }
      }
      for (Map.Entry<Byte, HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>> entry : mapJoinTables
          .entrySet()) {
        Byte pos = entry.getKey();
        HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashtable = entry.getValue();
        String filePath = Utilities.generatePath(baseDir, conf.getDumpFilePrefix(), pos, fileName);
        Path path = new Path(filePath);
        LOG.info("\tLoad back 1 hashtable file from tmp file uri:" + path.toString());
        hashtable.initilizePersistentHash(path.toUri().getPath());
      }
    } catch (Exception e) {
      LOG.error("Load Distributed Cache Error");
      throw new HiveException(e.getMessage());
    }
  }

  protected void handleFirstRow() throws HiveException, SerDeException {
    generateMapMetaData();
  }

  protected MapJoinObjectValue getValueFor(AbstractMapJoinKey key, Byte pos) {
    return mapJoinTables.get(pos).get(key);
  }

  // Load the hash table
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    try {
      if (firstRow) {
        // generate the map metadata
        generateMapMetaData();
        firstRow = false;
      }
      loadHashTable();
    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (mapJoinTables != null) {
      for (HashMapWrapper<?, ?> hashTable : mapJoinTables.values()) {
        hashTable.close();
      }
    }
    super.closeOp(abort);
  }

  /**
   * Implements the getName function for the Node Interface.
   *
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  public static String getOperatorName() {
    return "MAPJOIN";
  }
}
