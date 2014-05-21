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

package org.apache.hadoop.hive.metastore;

import junit.framework.Assert;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestRetention {

  private static HiveConf hiveConf;
  private static Hive db;

  @BeforeClass
  public static void start() throws Exception {

    System.setProperty("hive.metastore.retention.interval", "1s");
    int port = MetaStoreUtils.findFreePort();
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge());
    hiveConf = new HiveConf(TestRetention.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HMSHANDLERATTEMPTS, 2);
    hiveConf.setIntVar(HiveConf.ConfVars.HMSHANDLERINTERVAL, 0);
    hiveConf.setBoolVar(HiveConf.ConfVars.HMSHANDLERFORCERELOADCONF, false);

    db = Hive.get(hiveConf);
  }

  @AfterClass
  public static void stop() {
    Hive.closeCurrent();
  }

  @Test
  public void testTableRetention() throws Throwable {

    String tableName = "default.test_table";

    db.dropTable(tableName);
    Table tbl = db.newTable(tableName);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "string", "comment1"));
    cols.add(new FieldSchema("col2", "string", "comment2"));
    tbl.setFields(cols);

    tbl.setRetention(10); // seconds

    db.createTable(tbl);

    try {
      for (int i = 0; i < 12; i++) {
        Thread.sleep(1000);
        try {
          db.getTable(tableName);
        } catch (InvalidTableException e) {
          Assert.assertTrue("time index " + i, i >= 10);
          return;
        }
      }
      throw new Exception("Retention failed");
    } finally {
      db.dropTable(tableName);
    }
  }

  @Test
  public void testPartitionRetention() throws Throwable {

    String tableName = "default.test_table";

    db.dropTable(tableName);
    Table tbl = db.newTable(tableName);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "string", "comment1"));
    cols.add(new FieldSchema("col2", "string", "comment2"));
    tbl.setFields(cols);

    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    partCols.add(new FieldSchema("pcol", "string", "comment3"));
    tbl.setPartCols(partCols);

    tbl.setRetention(10); // seconds

    db.createTable(tbl);

    try {
      Map<String, String> partSpec = new LinkedHashMap<String, String>();
      partSpec.put("pcol", "v1");
      db.createPartition(tbl, partSpec);

      for (int i = 0; i < 12; i++) {
        Thread.sleep(1000);
        Partition partition = db.getPartition(tbl, partSpec, false);
        if (partition == null) {
          Assert.assertTrue("time index " + i, i >= 10);
          return;
        }
      }
      throw new Exception("Retention failed");
    } finally {
      db.dropTable(tableName);
    }
  }
}
