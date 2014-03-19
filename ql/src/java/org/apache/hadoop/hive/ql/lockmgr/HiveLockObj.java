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

package org.apache.hadoop.hive.ql.lockmgr;

public class HiveLockObj {

  final HiveLockObject obj;
  final HiveLockMode mode;

  public HiveLockObj(HiveLockObject obj, HiveLockMode mode) {
    this.obj  = obj;
    this.mode = mode;
  }

  public HiveLockObject getObj() {
    return obj;
  }

  public HiveLockMode getMode() {
    return mode;
  }

  public String getName() {
    return obj.getName();
  }

  public int hashCode() {
    return obj.hashCode();
  }

  public boolean equals(Object o) {
    if (!(o instanceof HiveLockObj)) {
      return false;
    }
    HiveLockObj other = (HiveLockObj) o;
    return obj.equals(other.obj);
  }

  public String toString() {
    return obj.getName() + "[" + mode + "]";
  }
}
