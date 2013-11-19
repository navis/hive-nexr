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

package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DelegatableAuthorizationProvider implements HiveAuthorizationProvider {

  private final HiveAuthorizationProvider delegated;

  public DelegatableAuthorizationProvider(HiveAuthorizationProvider delegated) {
    this.delegated = delegated;
  }

  @Override
  public void init(Configuration conf) throws HiveException {
    delegated.init(conf);
  }

  @Override
  public HiveAuthenticationProvider getAuthenticator() {
    return delegated.getAuthenticator();
  }

  @Override
  public void setAuthenticator(HiveAuthenticationProvider authenticator) {
    delegated.setAuthenticator(authenticator);
  }

  public void authorize(ReadEntity readEntity) throws HiveException, AuthorizationException {
    Set<ReadEntity> parents = readEntity.getParents();
    if (parents != null && !parents.isEmpty()) {
      return;
    }
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv, boolean grantedOnly) throws HiveException, AuthorizationException {
    if (owners == null) {
      delegated.authorize(readRequiredPriv, writeRequiredPriv, grantedOnly);
      return;
    }
    HiveAuthenticationProvider prev = delegated.getAuthenticator();
    try {
      for (String owner : owners) {
        delegated.setAuthenticator(new OwnerAuthenticationProvider(owner));
        delegated.authorize(readRequiredPriv, writeRequiredPriv, grantedOnly);
      }
    } finally {
      owners = null;
      delegated.setAuthenticator(prev);
    }
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv, boolean grantedOnly) throws HiveException, AuthorizationException {
    if (owners == null) {
      delegated.authorize(db, readRequiredPriv, writeRequiredPriv, grantedOnly);
      return;
    }
    HiveAuthenticationProvider prev = delegated.getAuthenticator();
    try {
      for (String owner : owners) {
        delegated.setAuthenticator(new OwnerAuthenticationProvider(owner));
        delegated.authorize(db, readRequiredPriv, writeRequiredPriv, grantedOnly);
      }
    } finally {
      owners = null;
      delegated.setAuthenticator(prev);
    }
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv, boolean grantedOnly) throws HiveException, AuthorizationException {
    if (owners == null) {
      System.out.println(table + ":" + delegated.getAuthenticator().getUserName() + " : " + Arrays.toString(readRequiredPriv) + ", " + Arrays.toString(writeRequiredPriv));
      delegated.authorize(table, readRequiredPriv, writeRequiredPriv, grantedOnly);
      return;
    }
    HiveAuthenticationProvider prev = delegated.getAuthenticator();
    try {
      for (String owner : owners) {
        System.out.println(table + ":" + owner + " : " + Arrays.toString(readRequiredPriv) + ", " + Arrays.toString(writeRequiredPriv));
        delegated.setAuthenticator(new OwnerAuthenticationProvider(owner));
        delegated.authorize(table, readRequiredPriv, writeRequiredPriv, grantedOnly);
      }
    } finally {
      owners = null;
      delegated.setAuthenticator(prev);
    }
  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv, boolean grantedOnly) throws HiveException, AuthorizationException {
    if (owners == null) {
      delegated.authorize(part, readRequiredPriv, writeRequiredPriv, grantedOnly);
      return;
    }
    HiveAuthenticationProvider prev = delegated.getAuthenticator();
    try {
      for (String owner : owners) {
        delegated.setAuthenticator(new OwnerAuthenticationProvider(owner));
        delegated.authorize(part, readRequiredPriv, writeRequiredPriv, grantedOnly);
      }
    } finally {
      owners = null;
      delegated.setAuthenticator(prev);
    }
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv, boolean grantedOnly) throws HiveException, AuthorizationException {
    if (owners == null) {
      System.out.println(table + ":" + delegated.getAuthenticator().getUserName() + " : " + Arrays.toString(readRequiredPriv) + ", " + Arrays.toString(writeRequiredPriv));
      delegated.authorize(table, part, columns, readRequiredPriv, writeRequiredPriv, grantedOnly);
      return;
    }
    HiveAuthenticationProvider prev = delegated.getAuthenticator();
    try {
      for (String owner : owners) {
        System.out.println(table + ":" + owner + " : " + Arrays.toString(readRequiredPriv) + ", " + Arrays.toString(writeRequiredPriv));
        delegated.setAuthenticator(new OwnerAuthenticationProvider(owner));
        delegated.authorize(table, part, columns, readRequiredPriv, writeRequiredPriv, grantedOnly);
      }
    } finally {
      owners = null;
      delegated.setAuthenticator(prev);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    delegated.setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return delegated.getConf();
  }

  public HiveAuthorizationProvider authorizerFor(ReadEntity readEntity) {
    owners = getOwner(readEntity);
    return this;
  }

  private volatile Set<String> owners;

  private Set<String> getOwner(ReadEntity readEntity) {
    Set<ReadEntity> parents = readEntity.getParents();
    if (parents == null || parents.isEmpty()) {
      return null;
    }
    Set<String> owners = new HashSet<String>();
    for (ReadEntity parent : parents) {
      if (parent.getType() == Entity.Type.TABLE) {
        owners.add(parent.getT().getTTable().getOwner());
      } else if (parent.getType() == Entity.Type.TABLE) {
        owners.add(parent.getP().getTable().getTTable().getOwner());
      }
    }
    if (owners.size() == 1 &&
        owners.iterator().next().equalsIgnoreCase(delegated.getAuthenticator().getUserName())) {
      return null;
    }
    return owners;
  }

  private static class OwnerAuthenticationProvider implements HiveAuthenticationProvider {

    private final String userName;

    private OwnerAuthenticationProvider(String userName) {
      this.userName = userName;
    }

    public String getUserName() {
      return userName;
    }

    public List<String> getGroupNames() {
      return null;
    }

    public void destroy() throws HiveException {
    }

    public void setConf(Configuration conf) {
    }

    public Configuration getConf() {
      return null;
    }
  }

}
