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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * For testing HiveServer in server mode
 *
 */
public class TestHiveServerSessions extends TestCase {

  private static final int clientNum = 2;

  private int port;
  private Thread server;

  private TSocket[] transports = new TSocket[clientNum];
  private HiveClient[] clients = new HiveClient[clientNum];

  private String tableName;
  private Path dataFilePath;

  public TestHiveServerSessions(String name) {
    super(name);
    HiveConf conf = new HiveConf(TestHiveServerSessions.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    tableName = "testsessionfunc";
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    port = findFreePort();
    server = new Thread(new Runnable() {
      public void run() {
        HiveServer.main(new String[]{"-p", String.valueOf(port)});
      }
    });
    server.start();
    Thread.sleep(1000);

    for (int i = 0; i < transports.length ; i++) {
      TSocket transport = new TSocket("localhost", port);
      transport.open();
      transports[i] = transport;
      clients[i] = new HiveClient(new TBinaryProtocol(transport));
      clients[i].execute("set hive.support.concurrency = false");
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
           // ignroe
         }
       }
     }
     if (server != null) {
      server.interrupt();
    }
  }

  private int findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
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

  public void testSessionFuncs() throws Exception {

    try {
      clients[0].execute("drop table " + tableName);
    } catch (Exception ex) {
    }
    clients[0].execute("create table " + tableName + " (key int, value string)");
    clients[0].execute("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName);

    clients[0].execute("create temporary function dummy as " +
        "'org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan'");
    clients[1].execute("create temporary function dummy as " +
        "'org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan'");

    clients[0].execute("select count(*) from " + tableName + " where dummy(key, 100)");
    assertEquals(84, Integer.valueOf(clients[0].fetchOne()).intValue());

    clients[1].execute("select count(*) from " + tableName + " where dummy(key, 100)");
    assertEquals(416, Integer.valueOf(clients[1].fetchOne()).intValue());
  }
}
