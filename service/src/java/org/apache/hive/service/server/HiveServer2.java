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

package org.apache.hive.service.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Log LOG = LogFactory.getLog(HiveServer2.class);
  private static CompositeServiceShutdownHook serverShutdownHook;
  public static final int SHUTDOWN_HOOK_PRIORITY = 100;

  private HiveConf hiveConf;
  private CLIService sqlService;
  private ThriftCLIService thriftSQLService;

  public HiveServer2() {
    super("HiveServer2");
    // TODO Auto-generated constructor stub
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;

    sqlService = new CLIService();
    addService(sqlService);

    thriftSQLService = new ThriftCLIService(sqlService);
    addService(thriftSQLService);

    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
    // TODO
  }

  @Override
  public synchronized void stop() {
    // TODO
    super.stop();
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);
    try {
      HiveConf hiveConf = new HiveConf();
      HiveServer2 server = new HiveServer2();
      server.init(hiveConf);
      server.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting HiveServer2", t);
      System.exit(-1);
    }
  }

}
