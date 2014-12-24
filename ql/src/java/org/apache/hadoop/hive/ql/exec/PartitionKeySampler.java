/**
 * Copyright 2010 The Apache Software Foundation
 *
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

public class PartitionKeySampler implements OutputCollector<HiveKey, Object> {

  private static final Log LOG = LogFactory.getLog(PartitionKeySampler.class);

  public static final Comparator<byte[]> C = new Comparator<byte[]>() {
    public final int compare(byte[] o1, byte[] o2) {
      return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
    }
  };

  private final List<byte[]> sampled = new ArrayList<byte[]>();

  public void addSampleFile(Path inputPath, JobConf job) throws IOException {
    FileSystem fs = inputPath.getFileSystem(job);
    FSDataInputStream input = fs.open(inputPath);
    try {
      int count = input.readInt();
      for (int i = 0; i < count; i++) {
        byte[] key = new byte[input.readInt()];
        input.readFully(key);
        sampled.add(key);
      }
    } finally {
      IOUtils.closeStream(input);
    }
  }

  // keys from FetchSampler are collected here
  public void collect(HiveKey key, Object value) throws IOException {
    sampled.add(Arrays.copyOfRange(key.getBytes(), 0, key.getLength()));
  }

  // sort and pick partition keys
  // originally copied from org.apache.hadoop.mapred.lib.InputSampler but seemed to have a bug
  private byte[][] getPartitionKeys(int numReduce) {
    if (sampled.size() < numReduce - 1) {
      throw new IllegalStateException("not enough number of sample");
    }
    byte[][] sorted = sampled.toArray(new byte[sampled.size()][]);
    Arrays.sort(sorted, C);

    return toPartitionKeys(sorted, numReduce);
  }

  static final byte[][] toPartitionKeys(byte[][] sorted, int numPartition) {
    byte[][] partitionKeys = new byte[numPartition - 1][];

    int last = 0;
    int current = 0;
    for(int i = 0; i < numPartition - 1; i++) {
      current += Math.round((float)(sorted.length - current) / (numPartition - i));
      while (i > 0 && current < sorted.length && C.compare(sorted[last], sorted[current]) == 0) {
        current++;
      }
      if (current >= sorted.length) {
        return Arrays.copyOfRange(partitionKeys, 0, i);
      }
      if (LOG.isDebugEnabled()) {
        // print out nth partition key for debugging
        LOG.debug("Partition key " + current + "th :" + new BytesWritable(sorted[current]));
      }
      partitionKeys[i] = sorted[current];
      last = current;
    }
    return partitionKeys;
  }

  public void writePartitionKeys(Path path, HiveConf conf, JobConf job) throws IOException {
    byte[][] partitionKeys = getPartitionKeys(job.getNumReduceTasks());
    int numPartition = partitionKeys.length + 1;
    if (numPartition != job.getNumReduceTasks()) {
      job.setNumReduceTasks(numPartition);
    }

    FileSystem fs = path.getFileSystem(job);
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, job, path,
        BytesWritable.class, NullWritable.class);
    try {
      for (byte[] pkey : partitionKeys) {
        BytesWritable wrapper = new BytesWritable(pkey);
        writer.append(wrapper, NullWritable.get());
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  public void sampling(FetchWork work, HiveConf conf, JobConf job) 
      throws HiveException, IOException {

    Operator operator = work.getSource();
    
    int sampleNum = conf.getIntVar(HiveConf.ConfVars.HIVESAMPLINGNUMBERFORORDERBY);
    float sampleRatio = conf.getFloatVar(HiveConf.ConfVars.HIVESAMPLINGPERCENTFORORDERBY);
    int numSplitsPerInput = conf.getIntVar(HiveConf.ConfVars.HIVESAMPLINGSPLITPERINPUTFORORDERBY);
    if (sampleRatio < 0.0 || sampleRatio > 1.0) {
      throw new IllegalArgumentException("Percentile value must be within the range of 0 to 1.");
    }

    FetchSampler sampler = new FetchSampler(
        work, job, operator, sampleNum, sampleRatio, numSplitsPerInput);
    try {
      operator.initialize(conf, new ObjectInspector[]{sampler.getOutputObjectInspector()});
      OperatorUtils.setChildrenCollector(operator.getChildOperators(), this);
      while (sampler.pushRow()) { }
    } finally {
      sampler.clearFetchContext();
    }
  }

  private class FetchSampler extends FetchOperator {

    private final Random random = new Random();
    
    private final int totalSampleNum;
    private final float ratio;
    private final int numSplitsPerInput;
    
    private int numInputs;
    private int currInput;
    private int numSamplePerInput;
    private int prvSampleForInput;
    
    private int numSplits;
    private int currSplit;
    private int numSamplePerSplit;
    private int prvSampleForSplit;

    public FetchSampler(FetchWork work, JobConf job, Operator<?> operator, 
        int totalSampleNum, float ratio, int numSplitsPerInput) throws HiveException {
      super(work, job, operator, null);
      this.totalSampleNum = totalSampleNum;
      this.ratio = ratio;
      this.numInputs = work.isPartitioned() ? work.getPartDir().size() : 1;
      this.numSplitsPerInput = numSplitsPerInput;

      if (LOG.isInfoEnabled()) {
        LOG.info("sample.total=" + totalSampleNum);
        LOG.info("sample.ratio=" + ratio);
        LOG.info("split.per.input=" + numSplitsPerInput);
      }
    }

    @Override
    protected InputSplit[] getSplitsFromPath(InputFormat formatter, JobConf job, Path path)
        throws IOException {
      currSplit = 0;
      numSamplePerInput = totalRemain() / (numInputs - currInput);
      prvSampleForInput = sampled.size();
      currInput++;

      InputSplit[] splits = formatter.getSplits(job, numSplitsPerInput);
      numSplits = splits.length;

      if (LOG.isDebugEnabled()) {
        LOG.debug("input.path=" + path + ", sample.per.input=" + numSamplePerInput);
      }
      return splits;
    }

    private int totalRemain() {
      return totalSampleNum - sampled.size();
    }

    private int inputRemain() {
      return numSamplePerInput - (sampled.size() - prvSampleForInput);
    }

    @Override
    protected FetchInputFormatSplit nextSplit() {
      numSamplePerSplit = inputRemain() / (numSplits - currSplit);
      prvSampleForSplit = sampled.size();
      currSplit++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("sample.per.split=" + numSamplePerSplit);
      }
      return super.nextSplit();
    }

    @Override
    protected void pushRow(InspectableObject row) throws HiveException, IOException {
      if (ratio <= 0 || random.nextFloat() < ratio) {
        super.pushRow(row);
        int sampleNum = sampled.size();
        if (sampleNum >= prvSampleForSplit + numSamplePerSplit) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("exceeded sample.per.split=" + (sampleNum - prvSampleForSplit));
          }
          skipCurrentSplit();
        } else if (sampleNum >= prvSampleForInput + numSamplePerInput) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("exceeded sample.per.input=" + (sampleNum - prvSampleForInput));
          }
          skipCurrentInput();
        } else if (sampleNum >= totalSampleNum) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("exceeded sample.total=" + sampleNum);
          }
          skipAll();
        }
      }
    }
  }
}
