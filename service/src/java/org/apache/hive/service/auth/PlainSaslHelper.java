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
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PlainSaslServer.ExternalAuthenticationCallback;
import org.apache.hive.service.auth.PlainSaslServer.SaslPlainProvider;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCLIService.Iface;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainSaslHelper {

  private static class PlainServerCallbackHandler implements CallbackHandler {

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      ExternalAuthenticationCallback ac = null;
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof ExternalAuthenticationCallback) {
          ac = (ExternalAuthenticationCallback) callbacks[i];
          break;
        } else {
          throw new UnsupportedCallbackException(callbacks[i]);
        }
      }

      if (ac != null) {
        PasswdAuthenticationProvider provider =
            AuthenticationProviderFactory.getAuthenticationProvider(ac.getAuthMethod());
        provider.Authenticate(ac.getUserName(), ac.getPasswd());
        ac.setAuthenticated(true);
      }
    }
  }

  public static class PlainClientbackHandler implements CallbackHandler {

    private final String userName;
    private final String passWord;

    public PlainClientbackHandler (String userName, String passWord) {
      this.userName = userName;
      this.passWord = passWord;
    }

    @Override
    public void handle(Callback[] callbacks)
          throws IOException, UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (int i = 0; i < callbacks.length; i++) {
        if (callbacks[i] instanceof NameCallback) {
          NameCallback nameCallback = (NameCallback)callbacks[i];
          nameCallback.setName(userName);
        } else if (callbacks[i] instanceof PasswordCallback) {
          PasswordCallback passCallback = (PasswordCallback) callbacks[i];
          passCallback.setPassword(passWord.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callbacks[i]);
        }
      }
    }
  }

  private static class SQLPlainProcessorFactory extends TProcessorFactory {
    private final ThriftCLIService service;
    private final HiveConf conf;

    public SQLPlainProcessorFactory(ThriftCLIService service) {
      super(null);
      this.service = service;
      this.conf = service.getHiveConf();
    }

    @Override
    public TProcessor getProcessor(TTransport trans) {
      TProcessor baseProcessor =  new TCLIService.Processor<Iface>(service);
      return new TSetIpAddressProcessor<Iface>(service);
    }
  }

  public static TProcessorFactory getPlainProcessorFactory(ThriftCLIService service) {
    return new SQLPlainProcessorFactory(service);
  }

  // Register Plain SASL server provider
  static {
    java.security.Security.addProvider(new SaslPlainProvider());
  }

  public static TTransportFactory getPlainTransportFactory(String authTypeStr, int saslMessageLimit) {
    if (saslMessageLimit > 0) {
      PlainSaslHelper.Factory saslFactory = new PlainSaslHelper.Factory(saslMessageLimit);
      saslFactory.addServerDefinition("PLAIN",
          authTypeStr, null, new HashMap<String, String>(),
          new PlainServerCallbackHandler());
      return saslFactory;
    }
    TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
    saslFactory.addServerDefinition("PLAIN",
        authTypeStr, null, new HashMap<String, String>(),
        new PlainServerCallbackHandler());
    return saslFactory;
  }

  public static TTransport getPlainTransport(String userName, String passwd,
      final TTransport underlyingTransport) throws SaslException {
    return new TSaslClientTransport("PLAIN", null,
        null, null, new HashMap<String, String>(),
        new PlainClientbackHandler(userName, passwd), underlyingTransport);
  }

  public static class Factory extends TTransportFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TSaslServerTransport.class);

    private final int saslMessageLimit;

    public Factory(int saslMessageLimit) {
      this.saslMessageLimit = saslMessageLimit;
    }

    private static class TSaslServerDefinition {
      public String mechanism;
      public String protocol;
      public String serverName;
      public Map<String, String> props;
      public CallbackHandler cbh;

      public TSaslServerDefinition(String mechanism, String protocol, String serverName,
                                   Map<String, String> props, CallbackHandler cbh) {
        this.mechanism = mechanism;
        this.protocol = protocol;
        this.serverName = serverName;
        this.props = props;
        this.cbh = cbh;
      }
    }

    private static Map<TTransport, WeakReference<TSaslServerTransport>> transportMap =
        Collections.synchronizedMap(
            new WeakHashMap<TTransport, WeakReference<TSaslServerTransport>>());

    private Map<String, TSaslServerDefinition> serverDefinitionMap =
        new HashMap<String, TSaslServerDefinition>();

    public void addServerDefinition(String mechanism, String protocol, String serverName,
                                    Map<String, String> props, CallbackHandler cbh) {
      serverDefinitionMap.put(mechanism, new TSaslServerDefinition(mechanism, protocol, serverName,
          props, cbh));
    }

    @Override
    public TTransport getTransport(TTransport base) {
      WeakReference<TSaslServerTransport> ret = transportMap.get(base);
      TSaslServerTransport transport = ret == null ? null : ret.get();
      if (transport == null) {
        LOGGER.debug("transport map does not contain key {}", base);
        transport = newSaslTransport(base);
        try {
          transport.open();
        } catch (TTransportException e) {
          LOGGER.debug("failed to open server transport", e);
          throw new RuntimeException(e);
        }
        transportMap.put(base, new WeakReference<TSaslServerTransport>(transport));
      } else {
        LOGGER.debug("transport map does contain key {}", base);
      }
      return transport;
    }

    private TSaslServerTransport newSaslTransport(final TTransport base) {
      TSaslServerTransport transport = new TSaslServerTransport(base) {

        private final byte[] messageHeader = new byte[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];

        @Override
        protected SaslResponse receiveSaslMessage() throws TTransportException {
          underlyingTransport.readAll(messageHeader, 0, messageHeader.length);

          byte statusByte = messageHeader[0];
          int length = EncodingUtils.decodeBigEndian(messageHeader, STATUS_BYTES);
          if (length > saslMessageLimit) {
            base.close();
            throw new TTransportException("Sasl message is too big (" + length + " bytes)");
          }
          byte[] payload = new byte[length];
          underlyingTransport.readAll(payload, 0, payload.length);

          NegotiationStatus status = NegotiationStatus.byValue(statusByte);
          if (status == null) {
            sendAndThrowMessage(NegotiationStatus.ERROR, "Invalid status " + statusByte);
          } else if (status == NegotiationStatus.BAD || status == NegotiationStatus.ERROR) {
            try {
              String remoteMessage = new String(payload, "UTF-8");
              throw new TTransportException("Peer indicated failure: " + remoteMessage);
            } catch (UnsupportedEncodingException e) {
              throw new TTransportException(e);
            }
          }
          if (LOGGER.isDebugEnabled())
            LOGGER.debug(getRole() + ": Received message with status {} and payload length {}",
                status, payload.length);
          return new SaslResponse(status, payload);
        }
      };
      for (Map.Entry<String, TSaslServerDefinition> entry : serverDefinitionMap.entrySet()) {
        TSaslServerDefinition definition = entry.getValue();
        transport.addServerDefinition(entry.getKey(),
            definition.protocol, definition.serverName, definition.props, definition.cbh);
      }
      return transport;
    }
  }
}
