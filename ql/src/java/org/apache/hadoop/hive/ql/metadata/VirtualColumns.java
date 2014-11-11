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

package org.apache.hadoop.hive.ql.metadata;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.VirtualColumn;
import org.apache.hadoop.hive.serde2.VirtualColumnProvider;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class VirtualColumns {

  private static final Log LOG = LogFactory.getLog(VirtualColumns.class.getName());

  private static final Function<VirtualColumn, String> FUNC_VC_NAME =
      new Function<VirtualColumn, String>() {
        @Override
        public String apply(VirtualColumn input) {
          return input.getName();
        }
      };

  private static final Predicate<VirtualColumn> PREDICATE_STATS_ONLY =
      new Predicate<VirtualColumn>() {
        @Override
        public boolean apply(VirtualColumn input) {
          return input.isStats();
        }
      };

  public static final ImmutableSet<VirtualColumn> VIRTUAL_COLUMNS =
      ImmutableSet.of(VirtualColumn.FILENAME, VirtualColumn.BLOCKOFFSET, VirtualColumn.ROWOFFSET,
      VirtualColumn.RAWDATASIZE, VirtualColumn.GROUPINGID, VirtualColumn.ROWID);

  public static final ImmutableSet<VirtualColumn> RUNTIME_VCS =
      ImmutableSet.of(VirtualColumn.FILENAME, VirtualColumn.BLOCKOFFSET, VirtualColumn.ROWOFFSET,
          VirtualColumn.GROUPINGID, VirtualColumn.ROWID);

  public static final ImmutableSet<String> VIRTUAL_COLUMN_NAMES = toNames(VIRTUAL_COLUMNS);

  public static List<VirtualColumn> getStatsRegistry(Configuration conf, Deserializer deserializer) {
    if (!(deserializer instanceof VirtualColumnProvider)) {
      return Collections.emptyList();
    }
    List<VirtualColumn> vcs = getCustomVCs(deserializer);
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_RAWDATASIZE)) {
      vcs.remove(VirtualColumn.RAWDATASIZE);
    }
    return ImmutableList.copyOf(Iterables.filter(vcs, PREDICATE_STATS_ONLY));
  }

  public static List<VirtualColumn> getRegistry(Configuration conf, Deserializer deserializer) {
    Set<VirtualColumn> nonStats = new LinkedHashSet<VirtualColumn>();
    nonStats.add(VirtualColumn.BLOCKOFFSET);
    nonStats.add(VirtualColumn.FILENAME);
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEROWOFFSET)) {
      nonStats.add(VirtualColumn.ROWOFFSET);
    }
    nonStats.add(VirtualColumn.ROWID);
    nonStats.addAll(getCustomVCs(deserializer));
    return ImmutableList.copyOf(Iterables.filter(nonStats, Predicates.not(PREDICATE_STATS_ONLY)));
  }

  public static ImmutableSet<String> toNames(Collection<VirtualColumn> registry) {
    return ImmutableSet.copyOf(Iterables.transform(registry, FUNC_VC_NAME));
  }

  public static Collection<String> removeVirtualColumns(Collection<String> columns) {
    Iterables.removeAll(columns, VIRTUAL_COLUMN_NAMES);
    return columns;
  }

  public static StructObjectInspector getVCSObjectInspector(List<VirtualColumn> vcs) {
    List<String> names = new ArrayList<String>(vcs.size());
    List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(vcs.size());
    for (VirtualColumn vc : vcs) {
      names.add(vc.getName());
      inspectors.add(vc.getObjectInspector());
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
  }

  private static List<VirtualColumn> getCustomVCs(Deserializer deserializer) {
    if (!(deserializer instanceof VirtualColumnProvider)) {
      return new ArrayList<VirtualColumn>();
    }
    List<VirtualColumn> vcs = ((VirtualColumnProvider) deserializer).getVirtualColumns();
    if (vcs == null || vcs.isEmpty()) {
      return new ArrayList<VirtualColumn>();
    }
    // to mutable
    vcs = new ArrayList<VirtualColumn>(vcs);
    if (vcs.removeAll(RUNTIME_VCS)) {
      LOG.warn(deserializer + " tried to override some runtime VCs, but ignored.");
    }
    return vcs;
  }

  public static class Builder {

    private final VirtualColumn[] vc;
    private final VirtualColumnProvider[] vcp;

    private final Object[] vcValues;

    private Builder(VirtualColumn[] vc, VirtualColumnProvider[] vcp) {
      this.vc = vc;
      this.vcp = vcp;
      this.vcValues = new Object[vc.length];
    }

    public Object[] evaluate() {
      for (int i = 0 ; i < vcp.length; i++) {
        vcValues[i] = vcp[i].evaluate(vc[i]);
      }
      return vcValues;
    }
  }

  public static Builder builder(List<VirtualColumn> vcs,
      VirtualColumnProvider defaultProvider, Deserializer deserializer) {
    if (defaultProvider == null) {
      defaultProvider = VirtualColumnProvider.NULL;
    }
    VirtualColumn[] vc = new VirtualColumn[vcs.size()];
    VirtualColumnProvider[] vcp = new VirtualColumnProvider[vcs.size()];

    Set<String> columnNames = toNames(getCustomVCs(deserializer));
    for (int i = 0 ; i < vcs.size(); i++) {
      vc[i] = vcs.get(i);
      vcp[i] = columnNames.contains(vc[i].getName()) ?
          (VirtualColumnProvider) deserializer : defaultProvider;
    }
    return new Builder(vc, vcp);
  }
}
