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

package org.apache.hadoop.hive.serde2;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.io.Serializable;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class VirtualColumn implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final VirtualColumn FILENAME =
      new VirtualColumn("INPUT__FILE__NAME", TypeInfoFactory.stringTypeInfo, false);
  public static final VirtualColumn BLOCKOFFSET =
      new VirtualColumn("BLOCK__OFFSET__INSIDE__FILE", TypeInfoFactory.longTypeInfo, false);
  public static final VirtualColumn ROWOFFSET =
      new VirtualColumn("ROW__OFFSET__INSIDE__BLOCK", TypeInfoFactory.longTypeInfo, false);
  public static final VirtualColumn RAWDATASIZE =
      new VirtualColumn("RAW__DATA__SIZE", TypeInfoFactory.longTypeInfo, true);

  /**
   * GROUPINGID is used with GROUP BY GROUPINGS SETS, ROLLUP and CUBE.
   * It composes a bit vector with the "0" and "1" values for every
   * column which is GROUP BY section. "1" is for a row in the result
   * set if that column has been aggregated in that row. Otherwise the
   * value is "0".  Returns the decimal representation of the bit vector.
   */
  public static final VirtualColumn GROUPINGID =
      new VirtualColumn("GROUPING__ID", TypeInfoFactory.intTypeInfo, false);

  /**
   * {@link RecordIdentifier}
   */
  public static final VirtualColumn ROWID = new VirtualColumn("ROW__ID",
      RecordIdentifier.StructInfo.typeInfo, true, false, RecordIdentifier.StructInfo.oi);

  private final String name;
  private final TypeInfo typeInfo;
  private final boolean isHidden;
  private final boolean isStats;
  private final ObjectInspector oi;

  public VirtualColumn(String name, PrimitiveTypeInfo typeInfo, boolean isStats) {
    this(name, typeInfo, true, isStats,
        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo));
  }

  public VirtualColumn(String name, TypeInfo typeInfo,
      boolean isHidden, boolean isStats, ObjectInspector oi) {
    this.name = name;
    this.typeInfo = typeInfo;
    this.isHidden = isHidden;
    this.isStats = isStats;
    this.oi = oi;
  }

  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  public String getName() {
    return this.name;
  }

  public boolean isHidden() {
    return isHidden;
  }

  public boolean getIsHidden() {
    return isHidden;
  }

  public boolean isStats() {
    return isStats;
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }

  @Override
  public int hashCode() {
    int c = 19;
    c = 31 * name.hashCode() + c;
    return 31 * typeInfo.hashCode() + c;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VirtualColumn)) {
      return false;
    }
    VirtualColumn c = (VirtualColumn) o;
    return name.equals(c.name) && typeInfo.equals(c.getTypeInfo());
  }
}
