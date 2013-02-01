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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Generic interface fro row fetching.
 * Sources can be single or multiple stream(s). The former(FetchOperator) simply reads row
 * from underlying InputFormat and the latter(MergeSortingFetcher) merges rows from
 * partially sorted stream into single sorted stream by comparing key(s) provided.
 *
 * Currently, MergeSortingFetcher is used for SMBJoin or bucketed result fetching for order by.
 */
public interface RowFetcher {

  /**
   * Setup context for fetching and return ObjectInspector for returning rows
   */
  ObjectInspector setupFetchContext() throws HiveException;

  /**
   * Fetch next row. Return null for EOF (no more row)
   */
  InspectableObject fetchRow() throws IOException, HiveException;

  /**
   * Fetch next row. Return null for EOF (no more row)
   */
  boolean pushRow() throws IOException, HiveException;

  /**
   * Clear context for fetching
   */
  void clearFetchContext() throws HiveException;
}
