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

package org.apache.hadoop.hive.metastore.retention;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MTable;

import java.util.List;

public class RetentionTarget {

  String databaseName;
  String tableName;
  List<String> partValues;
  String partNames;
  int createTime;
  int retention;

  public RetentionTarget(MTable table) {
    this.databaseName = table.getDatabase().getName();
    this.tableName = table.getTableName();
    this.createTime = table.getCreateTime();
    this.retention = table.getRetention();
  }

  public RetentionTarget(MPartition partition) throws MetaException {
    this.databaseName = partition.getTable().getDatabase().getName();
    this.tableName = partition.getTable().getTableName();
    this.partValues = partition.getValues();
    this.partNames = Warehouse.fieldsToPartName(partition.getTable().getPartitionKeys(), partValues);
    this.createTime = partition.getCreateTime();
    this.retention = partition.getTable().getRetention();
  }

  public String getName() {
    if (partValues == null) {
      return "table " + databaseName + "." + tableName;
    }
    return "partition " + databaseName + "." + tableName + "." + partNames;
  }

  public String toString() {
    return getName() + "[" + createTime + ":" + retention + "]";
  }
}
