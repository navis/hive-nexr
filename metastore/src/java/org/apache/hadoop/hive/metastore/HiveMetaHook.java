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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * HiveMetaHook defines notification methods which are invoked as part
 * of transactions against the metastore, allowing external catalogs
 * such as HBase to be kept in sync with Hive's metastore.
 *
 *<p>
 *
 * Implementations can use {@link MetaStoreUtils#isExternalTable} to
 * distinguish external tables from managed tables.
 */
public interface HiveMetaHook {
  /**
   * Called before a new table definition is added to the metastore
   * during CREATE TABLE.
   *
   * @param table new table definition
   */
  public void preCreateTable(Table table)
    throws MetaException;

  /**
   * Called after failure adding a new table definition to the metastore
   * during CREATE TABLE.
   *
   * @param table new table definition
   */
  public void rollbackCreateTable(Table table)
    throws MetaException;

  /**
   * Called after successfully adding a new table definition to the metastore
   * during CREATE TABLE.
   *
   * @param table new table definition
   */
  public void commitCreateTable(Table table)
    throws MetaException;

  /**
   * Called before a table definition is removed from the metastore
   * during DROP TABLE.
   *
   * @param table table definition
   */
  public void preDropTable(Table table)
    throws MetaException;

  /**
   * Called after failure removing a table definition from the metastore
   * during DROP TABLE.
   *
   * @param table table definition
   */
  public void rollbackDropTable(Table table)
    throws MetaException;

  /**
   * Called after successfully removing a table definition from the metastore
   * during DROP TABLE.
   *
   * @param table table definition
   *
   * @param deleteData whether to delete data as well; this should typically
   * be ignored in the case of an external table
   */
  public void commitDropTable(Table table, boolean deleteData)
    throws MetaException;

  /**
   * Called before a new partition definition is added to the metastore
   * during CREATE PARTITION.
   *
   * @param partition new partition definition
   */
  public void preCreatePartition(Table table, Partition partition)
    throws MetaException;

  /**
   * Called after failure adding a new partition definition to the metastore
   * during CREATE PARTITION.
   *
   * @param partition new partition definition
   */
  public void rollbackCreatePartition(Table table, Partition partition)
    throws MetaException;

  /**
   * Called after successfully adding a new partition definition to the metastore
   * during CREATE PARTITION.
   *
   * @param partition new partition definition
   */
  public void commitCreatePartition(Table table, Partition partition)
    throws MetaException;

  /**
   * Called before a partition definition is removed from the metastore
   * during DROP PARTITION.
   *
   * @param partition partition definition
   */
  public void preDropPartition(Table table, Partition partition)
    throws MetaException;

  /**
   * Called after failure removing a partition definition from the metastore
   * during DROP PARTITION.
   *
   * @param partition partition definition
   */
  public void rollbackDropPartition(Table table, Partition partition)
    throws MetaException;

  /**
   * Called after successfully removing a partition definition from the metastore
   * during DROP PARTITION.
   *
   * @param partition partition definition
   *
   * @param deleteData whether to delete data as well; this should typically
   * be ignored in the case of an external partition
   */
  public void commitDropPartition(Table table, Partition partition, boolean deleteData)
    throws MetaException;

  /**
   * Called for truncate table
   *
   * @param table table definition
   * @throws MetaException
   */
  public void truncateTable(Table table) throws MetaException;
}
