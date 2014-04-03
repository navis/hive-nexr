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

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

/**
 * A Hive Table Partition: is a fundamental storage unit within a Table.
 *
 * Please note that the ql code should always go through methods of this class to access the
 * metadata, instead of directly accessing org.apache.hadoop.hive.metastore.api.Partition.
 * This helps to isolate the metastore code and the ql code.
 */
public class Partition implements Serializable {

  @SuppressWarnings("nls")
  static final private Log LOG = LogFactory
      .getLog("hive.ql.metadata.Partition");

  private Table table;
  private boolean fullManaged;
  private org.apache.hadoop.hive.metastore.api.Partition tPartition;

  /**
   * These fields are cached. The information comes from tPartition.
   */
  private transient Deserializer deserializer;
  private transient Class<? extends HiveOutputFormat> outputFormatClass;
  private transient Class<? extends InputFormat> inputFormatClass;
  private transient Path path;
  /**

   * @return The values of the partition
   * @see org.apache.hadoop.hive.metastore.api.Partition#getValues()
   */
  public List<String> getValues() {
    return tPartition.getValues();
  }

  /**
   * Used only for serialization.
   */
  public Partition() {
  }

  /**
   * create an empty partition.
   * SemanticAnalyzer code requires that an empty partition when the table is not partitioned.
   */
  public Partition(Table tbl) throws HiveException {
    org.apache.hadoop.hive.metastore.api.Partition tPart =
        new org.apache.hadoop.hive.metastore.api.Partition();
    if (!tbl.isView()) {
      tPart.setSd(tbl.getTTable().getSd()); // TODO: get a copy
    }
    initialize(tbl, tPart);
  }

  public Partition(Table tbl, org.apache.hadoop.hive.metastore.api.Partition tp)
      throws HiveException {
    initialize(tbl, tp);
  }

  /**
   * Create partition object with the given info.
   *
   * @param tbl
   *          Table the partition will be in.
   * @param partSpec
   *          Partition specifications.
   * @param location
   *          Location of the partition, relative to the table.
   * @throws HiveException
   *           Thrown if we could not create the partition.
   */
  public Partition(Table tbl, Map<String, String> partSpec, Path location)
      throws HiveException {

    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartCols()) {
      String val = partSpec.get(field.getName());
      if (val == null) {
        throw new HiveException(
            "partition spec is invalid. field.getName() does not exist in input.");
      }
      pvals.add(val);
    }

    org.apache.hadoop.hive.metastore.api.Partition tpart = new org.apache.hadoop.hive.metastore.api.Partition();
    tpart.setDbName(tbl.getDbName());
    tpart.setTableName(tbl.getTableName());
    tpart.setValues(pvals);

    if (tbl.isView()) {
      initialize(tbl, tpart);
      return;
    }

    StorageDescriptor sd = new StorageDescriptor();
    try {
      // replace with THRIFT-138
      TMemoryBuffer buffer = new TMemoryBuffer(1024);
      TBinaryProtocol prot = new TBinaryProtocol(buffer);
      tbl.getTTable().getSd().write(prot);

      sd.read(prot);
    } catch (TException e) {
      LOG.error("Could not create a copy of StorageDescription");
      throw new HiveException("Could not create a copy of StorageDescription",e);
    }

    tpart.setSd(sd);
    if (location != null) {
      tpart.getSd().setLocation(location.toString());
    } else {
      tpart.getSd().setLocation(null);
    }

    initialize(tbl, tpart);
  }

  /**
   * Initializes this object with the given variables
   *
   * @param table
   *          Table the partition belongs to
   * @param tPartition
   *          Thrift Partition object
   * @throws HiveException
   *           Thrown if we cannot initialize the partition
   */
  private void initialize(Table table,
      org.apache.hadoop.hive.metastore.api.Partition tPartition) throws HiveException {

    this.table = table;
    this.fullManaged = table.isFullManaged();
    this.tPartition = tPartition;

    if (table.isView() || !isActivePartition()) {
      return;
    }

    try {
      StorageDescriptor partSD = tPartition.getSd();
      if (partSD.getLocation() == null) {
        // set default if location is not set and this is a physical
        // table partition (not a view partition)
        if (table.getDataLocation() != null) {
          String partName = Warehouse.makePartName(table.getPartCols(), tPartition.getValues());
          Path partPath = new Path(table.getDataLocation().toString(), partName);
          partSD.setLocation(partPath.toString());
        }
      }
      // set default if columns are not set
      if (partSD.getCols() == null || partSD.getCols().size() == 0) {
        if (table.getCols() != null) {
          partSD.setCols(table.getCols());
        }
      }
    } catch (MetaException e) {
      throw new HiveException("Invalid partition for table " + table.getTableName(), e);
    }

    // This will set up field: inputFormatClass
    getInputFormatClass();
    // This will set up field: outputFormatClass
    getOutputFormatClass();
  }

  public String getName() {
    try {
      return Warehouse.makePartName(table.getPartCols(), tPartition.getValues());
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  public Path[] getPath() {
    return new Path[]{getPartitionPath()};
  }

  public Path getPartitionPath() {
    if (path != null) {
      return path;
    }
    if (isActivePartition()) {
      if (tPartition.getSd() != null && tPartition.getSd().getLocation() != null) {
        return path = new Path(tPartition.getSd().getLocation());
      }
      return null;
    }
    Path location = new Path(table.getDataLocation());
    if (table.isPartitioned()) {
      return path = new Path(location, getName());
    }
    return path = location;
  }

  public final URI getDataLocation() {
    return getPartitionPath().toUri();
  }

  public final Deserializer getDeserializer(Properties props) throws HiveException {
    if (!isActivePartition()) {
      return table.getDeserializer();
    }
    try {
      if (deserializer == null) {
        deserializer = MetaStoreUtils.getDeserializer(Hive.get().getConf(), props);
      }
      return deserializer;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public Properties getMetadataFromPartitionSchema() {
    return isActivePartition() ?
      MetaStoreUtils.getPartitionMetadata(tPartition, table.getPartitionKeys()) :
      MetaStoreUtils.getTableMetadata(table.getTTable());
  }

  public Properties getSchemaFromTableSchema(Properties tblSchema) {
    return MetaStoreUtils.getPartSchemaFromTableSchema(table.getTTable().getSd(),
      table.getParameters(), table.getDbName(), table.getTableName(), table.getPartitionKeys(),
        tblSchema);
  }

  final public Class<? extends InputFormat> getInputFormatClass()
      throws HiveException {
    if (!isActivePartition()) {
      return table.getInputFormatClass();
    }
    if (inputFormatClass == null) {
      String clsName = getActiveSd().getInputFormat();
      if (clsName == null) {
        clsName = org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName();
      }
      try {
        inputFormatClass = ((Class<? extends InputFormat>) Class.forName(clsName, true,
            JavaUtils.getClassLoader()));
      } catch (ClassNotFoundException e) {
        throw new HiveException("Class not found: " + clsName, e);
      }
    }
    return inputFormatClass;
  }

  final public Class<? extends HiveOutputFormat> getOutputFormatClass()
      throws HiveException {
    if (!isActivePartition()) {
      return table.getOutputFormatClass();
    }
    if (outputFormatClass == null) {
      String clsName = getActiveSd().getOutputFormat();
      if (clsName == null) {
        clsName = HiveSequenceFileOutputFormat.class.getName();
      }
      try {
        Class<?> c = (Class.forName(clsName, true,
            JavaUtils.getClassLoader()));
        // Replace FileOutputFormat for backward compatibility
        if (!HiveOutputFormat.class.isAssignableFrom(c)) {
          outputFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(c);
        } else {
          outputFormatClass = (Class<? extends HiveOutputFormat>)c;
        }
      } catch (ClassNotFoundException e) {
        throw new HiveException("Class not found: " + clsName, e);
      }
    }
    return outputFormatClass;
  }

  public int getBucketCount() {
    return getActiveSd().getNumBuckets();
    /*
     * TODO: Keeping this code around for later use when we will support
     * sampling on tables which are not created with CLUSTERED INTO clause
     *
     * // read from table meta data int numBuckets = this.table.getNumBuckets();
     * if (numBuckets == -1) { // table meta data does not have bucket
     * information // check if file system has multiple buckets(files) in this
     * partition String pathPattern = this.partPath.toString() + "/*"; try {
     * FileSystem fs = FileSystem.get(this.table.getDataLocation(),
     * Hive.get().getConf()); FileStatus srcs[] = fs.globStatus(new
     * Path(pathPattern)); numBuckets = srcs.length; } catch (Exception e) {
     * throw new RuntimeException("Cannot get bucket count for table " +
     * this.table.getName(), e); } } return numBuckets;
     */
  }

  private StorageDescriptor getActiveSd() {
    if (isActivePartition() && tPartition.getSd() != null) {
      return tPartition.getSd();
    }
    return table.getTTable().getSd();
  }

  private boolean isActivePartition() {
    return table.isPartitioned() && !fullManaged;
  }

  public void setBucketCount(int newBucketNum) {
    if (!isActivePartition()) {
      throw new IllegalStateException("setBucketCount");
    }
    getActiveSd().setNumBuckets(newBucketNum);
  }

  public List<String> getBucketCols() {
    return getActiveSd().getBucketCols();
  }

  public List<Order> getSortCols() {
    return getActiveSd().getSortCols();
  }

  public List<String> getSortColNames() {
    return Utilities.getColumnNamesFromSortCols(getSortCols());
  }

  /**
   * get all paths for this partition in a sorted manner
   */
  @SuppressWarnings("nls")
  public FileStatus[] getSortedPaths() {
    try {
      // Previously, this got the filesystem of the Table, which could be
      // different from the filesystem of the partition.
      FileSystem fs = FileSystem.get(getPartitionPath().toUri(), Hive.get()
          .getConf());
      String pathPattern = getPartitionPath().toString();
      if (getBucketCount() > 0) {
        pathPattern = pathPattern + "/*";
      }
      LOG.info("Path pattern = " + pathPattern);
      FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
      Arrays.sort(srcs);
      for (FileStatus src : srcs) {
        LOG.info("Got file: " + src.getPath());
      }
      if (srcs.length == 0) {
        return null;
      }
      return srcs;
    } catch (Exception e) {
      throw new RuntimeException("Cannot get path ", e);
    }
  }

  /**
   * mapping from bucket number to bucket path
   */
  // TODO: add test case and clean it up
  @SuppressWarnings("nls")
  public Path getBucketPath(int bucketNum) {
    FileStatus srcs[] = getSortedPaths();
    if (srcs == null) {
      return null;
    }
    return srcs[bucketNum].getPath();
  }

  public LinkedHashMap<String, String> getSpec() {
    return table.createSpec(tPartition);
  }

  @SuppressWarnings("nls")
  @Override
  public String toString() {
    String pn = "Invalid Partition";
    try {
      pn = Warehouse.makePartName(getSpec(), false);
    } catch (MetaException e) {
      // ignore as we most probably in an exception path already otherwise this
      // error wouldn't occur
    }
    return table.toString() + "(" + pn + ")";
  }

  public Table getTable() {
    return table;
  }

  /**
   * Should be only used by serialization.
   */
  public void setTable(Table table) {
    this.table = table;
  }

  /**
   * Should be only used by serialization.
   */
  public org.apache.hadoop.hive.metastore.api.Partition getTPartition() {
    return tPartition;
  }

  public Map<String, String> getParameters() {
    return isActivePartition() ? tPartition.getParameters() : table.getParameters();
  }

  public List<FieldSchema> getCols() {
    return getActiveSd().getCols();
  }

  public String getLocation() {
    Path path = getPartitionPath();
    return path == null ? null : path.toString();
  }

  public void setLocation(String location) {
    if (!isActivePartition()) {
      throw new IllegalStateException("setLocation");
    }
    tPartition.getSd().setLocation(location);
    path = null;
  }

  /**
   * Set Partition's values
   *
   * @param partSpec
   *          Partition specifications.
   * @throws HiveException
   *           Thrown if we could not create the partition.
   */
  public void setValues(Map<String, String> partSpec)
      throws HiveException {
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : table.getPartCols()) {
      String val = partSpec.get(field.getName());
      if (val == null) {
        throw new HiveException(
            "partition spec is invalid. field.getName() does not exist in input.");
      }
      pvals.add(val);
    }
    tPartition.setValues(pvals);
  }

  /**
   * @param protectMode
   */
  public void setProtectMode(ProtectMode protectMode){
    if (!isActivePartition()) {
      throw new IllegalStateException("setProtectMode");
    }
    Map<String, String> parameters = tPartition.getParameters();
    String pm = protectMode.toString();
    if (pm != null) {
      parameters.put(ProtectMode.PARAMETER_NAME, pm);
    } else {
      parameters.remove(ProtectMode.PARAMETER_NAME);
    }
    tPartition.setParameters(parameters);
  }

  /**
   * @return protect mode
   */
  public ProtectMode getProtectMode(){
    if (!isActivePartition()) {
      return table.getProtectMode();
    }
    Map<String, String> parameters = tPartition.getParameters();

    if (parameters == null) {
      return null;
    }

    if (!parameters.containsKey(ProtectMode.PARAMETER_NAME)) {
      return new ProtectMode();
    } else {
      return ProtectMode.getProtectModeFromString(
          parameters.get(ProtectMode.PARAMETER_NAME));
    }
  }

  /**
   * @return True protect mode indicates the partition if offline.
   */
  public boolean isOffline(){
    ProtectMode pm = getProtectMode();
    return pm != null && pm.offline;
  }

  /**
   * @return True if protect mode attribute of the partition indicate
   * that it is OK to drop the table
   */
  public boolean canDrop() {
    ProtectMode mode = getProtectMode();
    ProtectMode parentMode = table.getProtectMode();
    return (!mode.noDrop && !mode.offline && !mode.readOnly && !parentMode.noDropCascade);
  }

  /**
   * @return True if protect mode attribute of the partition indicate
   * that it is OK to write to the table
   */
  public boolean canWrite() {
    ProtectMode mode = getProtectMode();
    return (!mode.offline && !mode.readOnly);
  }

  /**
   * @return include the db name
   */
  public String getCompleteName() {
    return getTable().getCompleteName() + "@" + getName();
  }

  public int getLastAccessTime() {
    return tPartition.getLastAccessTime();
  }

  public void setLastAccessTime(int lastAccessTime) {
    tPartition.setLastAccessTime(lastAccessTime);
  }

  public boolean isStoredAsSubDirectories() {
    return getActiveSd().isStoredAsSubDirectories();
  }

  public List<List<String>> getSkewedColValues(){
    return getActiveSd().getSkewedInfo().getSkewedColValues();
  }

  public List<String> getSkewedColNames() {
    return getActiveSd().getSkewedInfo().getSkewedColNames();
  }

  public void setSkewedValueLocationMap(List<String> valList, String dirName)
      throws HiveException {
    if (!isActivePartition()) {
      throw new IllegalStateException("setSkewedValueLocationMap");
    }
    Map<List<String>, String> mappings = getActiveSd().getSkewedInfo()
        .getSkewedColValueLocationMaps();
    if (null == mappings) {
      mappings = new HashMap<List<String>, String>();
      getActiveSd().getSkewedInfo().setSkewedColValueLocationMaps(mappings);
    }

    // Add or update new mapping
    mappings.put(valList, dirName);
  }

  public Map<List<String>, String> getSkewedColValueLocationMaps() {
    return getActiveSd().getSkewedInfo().getSkewedColValueLocationMaps();
  }
}
