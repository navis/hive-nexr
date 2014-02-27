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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

/**
 * This class encapsulates the information on the partition and tables that are
 * read by the query.
 */
public class ReadEntity extends Entity implements Serializable {

  // Consider a query like: select * from V, where the view V is defined as:
  // select * from T
  // The inputs will contain V and T (parent: V)

  // For views, the entities can be nested - by default, entities are at the top level
  private final Set<ReadEntity> parents = new HashSet<ReadEntity>();

  private transient Operator<?> source;

  /**
   * For serialization only.
   */
  public ReadEntity() {
    super();
  }

  /**
   * Constructor for a database.
   */
  public ReadEntity(Database database) {
    super(database, true);
  }

  public ReadEntity(Database database, HiveOperation operation) {
    super(database, true);
    setInputRequiredPrivileges(operation.getInputRequiredPrivileges());
    setOutputRequiredPrivileges(operation.getOutputRequiredPrivileges());
  }

  /**
   * Constructor.
   *
   * @param t
   *          The Table that the query reads from.
   */
  public ReadEntity(Table t) {
    super(t, true);
  }

  public ReadEntity(Table t, HiveOperation operation) {
    super(t, true);
    setInputRequiredPrivileges(operation.getInputRequiredPrivileges());
    setOutputRequiredPrivileges(operation.getOutputRequiredPrivileges());
  }

  private void initParent(ReadEntity parent) {
    if (parent != null) {
      this.parents.add(parent);
    }
  }

  public ReadEntity(Table t, Operator<?> source, ReadEntity parent) {
    super(t, true);
    initParent(parent);
    this.source = source;
  }

  /**
   * Constructor given a partition.
   *
   * @param p
   *          The partition that the query reads from.
   */
  public ReadEntity(Partition p) {
    super(p, true);
  }

  public ReadEntity(Partition p, HiveOperation operation) {
    super(p, true);
    setInputRequiredPrivileges(operation.getInputRequiredPrivileges());
    setOutputRequiredPrivileges(operation.getOutputRequiredPrivileges());
  }

  public ReadEntity(Partition p, Operator<?> source, ReadEntity parent) {
    super(p, true);
    initParent(parent);
    this.source = source;
  }

  public Set<ReadEntity> getParents() {
    return parents;
  }

  public List<FieldSchema> getColumnRefs() {
    if (getTable() == null || getTable().isView()) {
      return null;
    }
    List<FieldSchema> fields = getTable().getCols();
    if (source instanceof TableScanOperator) {
      List<Integer> columns = ((TableScanOperator)source).getNeededColumnIDs();
      if (columns != null) {
        List<FieldSchema> copy = new ArrayList<FieldSchema>();
        for (int index : columns) {
          copy.add(fields.get(index));
        }
        return copy;
      }
    }
    return new ArrayList<FieldSchema>(fields);
  }

  public List<String> getColumnRefNames() {
    List<FieldSchema> refs = getColumnRefs();
    if (refs == null) {
      return null;
    }
    List<String> columns = new ArrayList<String>();
    for (FieldSchema ref : refs) {
      columns.add(ref.getName());
    }
    return columns;
  }

  /**
   * Equals function.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof ReadEntity) {
      ReadEntity ore = (ReadEntity) o;
      return (toString().equalsIgnoreCase(ore.toString()));
    } else {
      return false;
    }
  }
}
