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

package org.apache.hadoop.hive.common;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.List;

public class SessionBase {

  /**
   * Singleton Session object per thread.
   *
   **/
  protected static final ThreadLocal<SessionBase> tss = new ThreadLocal<SessionBase>();

  /**
   * current configuration.
   */
  protected final HiveConf conf;

  /**
   * current configuration.
   */
  protected final String userName;

  protected final List<String> groupNames;

  protected String currentDB;

  public SessionBase(HiveConf conf, String userName, List<String> groupNames) {
    this.conf = conf;
    this.userName = userName;
    this.groupNames = groupNames;
  }

  public HiveConf getConf() {
    return conf;
  }

  public String getUserName() {
    return userName;
  }

  public List<String> getGroupNames() {
    return groupNames;
  }

  public String getCurrentDB() {
    return currentDB;
  }

  public void setCurrentDB(String currentDB) {
    this.currentDB = currentDB;
  }

  public static SessionBase get() {
    return tss.get();
  }
}
