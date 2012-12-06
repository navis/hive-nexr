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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 * HiveKey is a simple wrapper on Text which allows us to set the hashCode
 * easily. hashCode is used for hadoop partitioner.
 */
public class HiveKey extends BytesWritable {

  private static final int LENGTH_BYTES = 4;

  private int hashCode;
  private boolean hashCodeValid;

  private transient int distKeyLength;

  transient int skewGroup = -1;
  transient int skewOffset = -1;

  transient boolean[] drivers;
  transient int[] skewGroupSize;   // size of partition for skewed group
  transient int[] skewGroupStart;  // starting number of partition for skewed group

  public HiveKey() {
    hashCodeValid = false;
  }

  public HiveKey(byte[] bytes, int hashcode) {
    super(bytes);
    hashCode = hashcode;
    hashCodeValid = true;
  }

  public void setHashCode(int myHashCode) {
    hashCodeValid = true;
    hashCode = myHashCode;
  }

  public void setSkewGroup(int skewGroup) {
    this.skewGroup = skewGroup;
  }

  public int getSkewGroup() {
    return skewGroup;
  }

  public boolean initializeSkew(int numReduceTasks, boolean[] skewDrivers, int[] skewClusters) {
    drivers = skewDrivers;
    skewGroupSize = getSkewAssigned(numReduceTasks, skewClusters);
    skewGroupStart = getSkewOffsets(numReduceTasks, skewGroupSize);
    return skewGroupSize != null;
  }

  public boolean isSkewed() {
    return skewGroupSize != null;
  }

  @SuppressWarnings("unchecked")
  public void collect(OutputCollector out, Object value) throws IOException {
    if (skewGroup >= 0 && !drivers[skewGroup] && skewGroupSize[skewGroup] > 1) {
      // non-driving skewed key. will be propagated to all partitions in the group
      for (int i = 0; i < skewGroupSize[skewGroup]; i++) {
        skewOffset = i;   // this is referenced by SkewedKeyPartitioner
        out.collect(this, value);
      }
      skewOffset = -1;
    } else {
      out.collect(this, value);
    }
  }

  // calculates number of reducers for each group
  //   numReduceTasks : total number of reducer
  //   skewedSize : size of each group, >= 0 means percent, < 0 means constant number
  //
  // remaining reducers are assigned to non-skewed keys (skewGroup = -1)
  // if remaining reducers is less than one, skew join will be disabled and back to normal join
  private int[] getSkewAssigned(int numReduceTasks, int[] skewedSize) {
    int remain = numReduceTasks;
    int[] assigned = new int[skewedSize.length];
    for (int i = 0; i < assigned.length; i++) {
      remain -= assigned[i] = skewedSize[i] < 0 ? -skewedSize[i] :
          Math.max(1, (int)(numReduceTasks * (skewedSize[i] / 100.0f)));
    }
    return remain < 1 ? null : assigned;
  }

  // calculate start offset for each group
  // for non-skewed group, partition number is 0 ~ offset[0]
  // for skew group n, partition number is offset[n] ~ offset[n] - 1
  private int[] getSkewOffsets(int numReduceTasks, int[] assigned) {
    if (assigned == null) {
      return null;
    }
    int remain = numReduceTasks;
    int[] offsets = new int[assigned.length];
    for (int i = offsets.length - 1; i >= 0; i--) {
      offsets[i] = remain -= assigned[i];
    }
    return offsets;
  }

  @Override
  public int hashCode() {
    if (!hashCodeValid) {
      throw new RuntimeException("Cannot get hashCode() from deserialized "
          + HiveKey.class);
    }
    return hashCode;
  }

  public void setDistKeyLength(int distKeyLength) {
    this.distKeyLength = distKeyLength;
  }

  public int getDistKeyLength() {
    return distKeyLength;
  }

  /** A Comparator optimized for HiveKey. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(HiveKey.class);
    }

    /**
     * Compare the buffers in serialized form.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2
          + LENGTH_BYTES, l2 - LENGTH_BYTES);
    }
  }

  static {
    WritableComparator.define(HiveKey.class, new Comparator());
  }
}
