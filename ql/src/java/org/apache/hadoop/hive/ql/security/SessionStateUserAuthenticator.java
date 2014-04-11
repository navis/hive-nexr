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

package org.apache.hadoop.hive.ql.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.List;

/**
 * Authenticator that returns the userName set in SessionState. For use when authorizing with HS2
 * so that HS2 can set the user for the session through SessionState
 */
public class SessionStateUserAuthenticator implements HiveMetastoreAuthenticationProvider {

  private final List<String> groupNames = new ArrayList<String>();

  protected Configuration conf;

  @Override
  public List<String> getGroupNames() {
    return groupNames;
  }

  @Override
  public String getUserName() {
    SessionState session = SessionState.get();
    String username = session == null ? null : session.getUserName();
    if (username == null) {
      Configuration conf = session == null ? this.conf : session.getConf();
      return HadoopDefaultAuthenticator.getUserGroupInformation(conf).getUserName();
    }
    return username;
  }

  @Override
  public void destroy() throws HiveException {
    return;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void setMetaStoreHandler(HiveMetaStore.HMSHandler handler) {
    this.conf = handler.getConf();
  }
}
