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

/**
 * Assigns partition number by precalculated skewGroup and skewOffset
 *
 * 1. skewGroup < 0 (non skewed)
 * 2. skewGroup >= 0 and skewOffset >= 0 (skewed, non-driving)
 * 3. skewGroup >= 0 and skewOffset < 0 (skewed, driving)
 */
public class SkewedKeyPartitioner<V> extends DefaultHivePartitioner<HiveKey, V> {

  @Override
  public int getPartition(HiveKey key, V value, int numReduceTasks) {
    if (!key.isSkewed()) {
      // normal partitioning
      return super.getPartition(key, value, numReduceTasks);
    }
    if (key.skewGroup < 0) {
      return super.getPartition(key, value, key.skewGroupStart[0]); // case 1
    }
    int groupSize = key.skewGroupSize[key.skewGroup];
    int groupStart = key.skewGroupStart[key.skewGroup];
    if (groupSize == 1) {
      return groupStart;
    }
    if (key.skewOffset >= 0) {
      return groupStart + key.skewOffset;      // case 2
    }
    return groupStart + super.getPartition(key, value, groupSize);  // case 3
  }
}
