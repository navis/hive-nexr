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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class RandomSampler {

  public static final Comparator<byte[]> C = new Comparator<byte[]>() {
    public int compare(byte[] o1, byte[] o2) {
      int compare = WritableComparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
      return compare == 0 ? 1 : compare;  // no overwrite
    }
  };

  private final Random random = new Random(1234);
  private final byte[][] sampled;
  private final ExprNodeEvaluator[] evals;
  private final StructObjectInspector keyOI;
  private final Serializer serializer;

  private int counter;
  private int threshold;
  private float ratio;

  private int size;
  private transient Object[] keys;

  public RandomSampler(ExprNodeEvaluator[] evals, StructObjectInspector keyOI,
      Serializer serializer, int sampleNum) {
    this.evals = evals;
    this.keyOI = keyOI;
    this.serializer = serializer;
    this.sampled = new byte[sampleNum][];
    this.keys = new Object[evals.length];
    threshold = sampleNum;
    ratio = 1;
  }

  public void sampling(Object row, StructObjectInspector rowOI)
      throws SerDeException, HiveException {
    if (size < sampled.length || random.nextFloat() < ratio) {
      for (int i = 0; i < evals.length; i++) {
        keys[i] = evals[i].evaluate(row);
      }
      BytesWritable sample = ((BytesWritable) serializer.serialize(keys, keyOI));
      addSample(Arrays.copyOfRange(sample.getBytes(), 0, sample.getLength()));
    }
    if (++counter == threshold) {
      threshold *= 10;
      ratio /= 10f;
    }
  }

  public void addSample(byte[] sample) {
    int target = Arrays.binarySearch(sampled, 0, size, sample, C);
    int index = target < 0 ? -target - 1 : target;
    if (size == sampled.length) {
      index = removeRandom(target, index);
    } else if (size > 0) {
      index = shift(index);
    }
    sampled[index] = sample;
    size++;
  }

  private int removeRandom(int target, int index) {
    int remove = random.nextInt(sampled.length - 2) + 1;
    if (remove == target) {
      size--;
      return target;
    }
    byte[] prev = sampled[remove];
    if (remove < index) {
      if (target < 0) {
        System.arraycopy(sampled, remove + 1, sampled, remove, index - remove - 1);
        index--;
      } else {
        System.arraycopy(sampled, remove + 1, sampled, remove, index - remove);
      }
    } else {
      System.arraycopy(sampled, index, sampled, index + 1, remove - index);
    }
    sampled[index] = prev;
    size--;
    return index;
  }

  private int shift(int index) {
    byte[] removed = sampled[sampled.length - 1];
    System.arraycopy(sampled, index, sampled, index + 1, size - index);
    sampled[index] = removed;
    return index;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        builder.append(',');
      }
      builder.append(sampled[i] == null ? '-' : new String(sampled[i]));
    }
    return builder.toString();
  }

  public void publish(JobConf jc, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(jc);
    FSDataOutputStream fout = fs.create(path);
    fout.writeInt(size);
    for (int i = 0; i < size; i++) {
      fout.writeInt(sampled[i].length);
      fout.write(sampled[i]);
    }
    fout.close();
  }
}
