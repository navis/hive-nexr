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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;

public class HadoopDefaultAuthenticator implements HiveAuthenticationProvider {

  private String userName;
  private List<String> groupNames;

  private Configuration conf;

  @Override
  public List<String> getGroupNames() {
    return groupNames;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    UserGroupInformation ugi = getUserGroupInformation(conf);

    this.userName = ugi.getUserName();

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_SKIP_GROUPNAMES)) {
      return;
    }
    if (ugi.getGroupNames() != null) {
      this.groupNames = Arrays.asList(ugi.getGroupNames());
    }
  }

  public static UserGroupInformation getUserGroupInformation(Configuration conf) {
    UserGroupInformation ugi;
    try {
      ugi = ShimLoader.getHadoopShims().getUGIForConf(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (ugi == null) {
      throw new RuntimeException(
          "Can not initialize HadoopDefaultAuthenticator.");
    }
    return ugi;
  }

  @Override
  public void destroy() throws HiveException {
    return;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

}
