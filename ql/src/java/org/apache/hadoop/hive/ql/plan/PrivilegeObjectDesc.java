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

import java.util.HashMap;
import java.util.List;

@Explain(displayName="privilege subject")
public class PrivilegeObjectDesc {

  private String database;
  private String table;
  private HashMap<String, String> partSpec;

  private List<String> columns;

  public PrivilegeObjectDesc() {
  }

  public PrivilegeObjectDesc(String database, String table,
      HashMap<String, String> partSpec, List<String> columns) {
    this.database = database;
    this.table = table;
    this.partSpec = partSpec;
    this.columns = columns;
  }

  @Explain(displayName="object")
  public String getPrivilegeObject() {
    StringBuilder builder = new StringBuilder();
    if (database != null) {
      builder.append(database);
    }
    if (table != null) {
      builder.append('.').append(table);
    }
    if (partSpec != null) {
      builder.append(partSpec);
    }
    return builder.toString();
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public HashMap<String, String> getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(HashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  public List<String> getColumns() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }
}
