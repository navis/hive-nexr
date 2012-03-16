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
package org.apache.hadoop.hive.service;

import junit.framework.TestCase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * For testing HiveServer in server mode
 *
 */
public class TestHiveServerSessions extends TestCase {

  private static final int clientNum = 2;

  private TServer server;

  private TSocket[] transports = new TSocket[clientNum];
  private HiveClient[] clients = new HiveClient[clientNum];

  public TestHiveServerSessions(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    int port = findFreePort();
    server = HiveServer.run(new String[]{"-p", String.valueOf(port)});
    Thread runner = new Thread(new Runnable() {
      public void run() {
          server.serve();
      }
    });
    runner.setDaemon(true);
    runner.start();

    Thread.sleep(1000);

    for (int i = 0; i < transports.length ; i++) {
      TSocket transport = new TSocket("localhost", port);
      transport.open();
      transports[i] = transport;
      clients[i] = new HiveClient(new TBinaryProtocol(transport));
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    for (TSocket socket : transports) {
      if (socket != null) {
        try {
          socket.close();
        } catch (Exception e) {
          // ignore
        }
      }
    }
    if (server != null) {
      server.stop();
    }
  }

  private int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    try {
      int port = socket.getLocalPort();
      if (port < 0) {
        throw new IOException("failed to find free port");
      }
      return port;
    } finally {
      socket.close();
    }
  }

  public void testSessionVars() throws Exception {
    for (int i = 0; i < clients.length; i++) {
      clients[i].execute("set hiveconf:var=value" + i);
    }

    for (int i = 0; i < clients.length; i++) {
      clients[i].execute("set hiveconf:var");
      assertEquals("hiveconf:var=value" + i, clients[i].fetchOne());
    }
  }

  public static void main(String[] args) throws Exception{
    TestHiveServerSessions test = new TestHiveServerSessions("TestHiveServerSessions");
    try {
      test.setUp();
      test.testSessionVars();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      test.tearDown();
    }
  }
}
