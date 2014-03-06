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
package org.apache.hive.service.auth;

import java.io.IOException;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

public class HiveAuthFactory {

  public static enum AuthTypes {
    NOSASL("NOSASL"),
    NONE("NONE"),
    LDAP("LDAP"),
    KERBEROS("KERBEROS"),
    CUSTOM("CUSTOM");

    private String authType; // Auth type for SASL

    AuthTypes(String authType) {
      this.authType = authType;
    }

    public String getAuthName() {
      return authType;
    }

  };

  private final HadoopThriftAuthBridge.Server saslServer;
  private final String authTypeStr;
  private final AuthTypes authType;
  private final int saslMessageLimit;
  private final HiveConf conf;

  public HiveAuthFactory() throws TTransportException {
    conf = new HiveConf();

    String config = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION);
    if (config == null) {
      authType = AuthTypes.NONE;
      authTypeStr = AuthTypes.NONE.getAuthName();
    } else {
      authType = AuthTypes.valueOf(config.toUpperCase());;
      authTypeStr = config;
    }
    if (authType == AuthTypes.KERBEROS
        && ShimLoader.getHadoopShims().isSecureShimImpl()) {
      saslServer = ShimLoader.getHadoopThriftAuthBridge().createServer(
        conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB),
        conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
        );
    } else {
      saslServer = null;
    }
    saslMessageLimit = conf.getIntVar(ConfVars.HIVE_SERVER2_SASL_MESSAGE_LIMIT);
  }

  public TTransportFactory getAuthTransFactory() throws LoginException {
    if (authType == AuthTypes.KERBEROS) {
      try {
        return saslServer.createTransportFactory();
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
    }
    return PlainSaslHelper.getPlainTransportFactory(authTypeStr, saslMessageLimit);
  }

  public TProcessorFactory getAuthProcFactory(ThriftCLIService service)
      throws LoginException {
    if (authType == AuthTypes.KERBEROS) {
      return KerberosSaslHelper.getKerberosProcessorFactory(saslServer, service);
    } else {
      return PlainSaslHelper.getPlainProcessorFactory(service);
    }
  }

  public String getRemoteUser() {
    if (saslServer != null) {
      return saslServer.getRemoteUser();
    } else {
      return null;
    }
  }

  /* perform kerberos login using the hadoop shim API if the configuration is available */
  public static void loginFromKeytab(HiveConf hiveConf) throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    if (principal.isEmpty() && keyTabFile.isEmpty()) {
      // no security configuration available
      return;
    } else if (!principal.isEmpty() && !keyTabFile.isEmpty()) {
      ShimLoader.getHadoopShims().loginUserFromKeytab(principal, keyTabFile);
    } else {
      throw new IOException ("HiveServer2 kerberos principal or keytab is not correctly configured");
    }
  }

}
