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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.thrift.ThriftCLIService;

/**
 * HiveServer2.
 *
 */
public class HiveServer2 extends CompositeService {
  private static final Log LOG = LogFactory.getLog(HiveServer2.class);
  private static CompositeServiceShutdownHook serverShutdownHook;
  public static final int SHUTDOWN_HOOK_PRIORITY = 100;

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;

  public HiveServer2() {
    super("HiveServer2");
  }


  @Override
  public synchronized void init(HiveConf hiveConf) {
    cliService = new CLIService();
    addService(cliService);

    thriftCLIService = new ThriftCLIService(cliService);
    addService(thriftCLIService);

    super.init(hiveConf);
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  /**
   * @param args
   */
  public static void main(String[] args) {

    //NOTE: It is critical to do this here so that log4j is reinitialized
    // before any of the other core hive classes are loaded
    try {
      LogUtils.initHiveLog4j();
    } catch (LogInitializationException e) {
      LOG.warn(e.getMessage());
    }

    HiveStringUtils.startupShutdownMessage(HiveServer2.class, args, LOG);
    HiveServer.HiveServerCli cli = new HiveServer.HiveServerCli();
    cli.parse(args);

    HiveConf hiveConf = new HiveConf();
    try {
      Properties hiveconf = cli.addHiveconfToSystemProperties();
      for (Map.Entry<Object, Object> item : hiveconf.entrySet()) {
        hiveConf.set((String) item.getKey(), (String) item.getValue());
      }
      HiveServer2 server = new HiveServer2();
      server.init(hiveConf);

      server.initialize(cli.initFiles, cli.registerServerResources);
      server.validateJars(hiveConf);
      server.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting HiveServer2", t);
      System.exit(-1);
    }
  }

  private void validateJars(HiveConf conf) throws IOException {
    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
    if (auxJars != null && !auxJars.isEmpty()) {
      for (String auxJar : auxJars.split(",")) {
        Path path = new Path(auxJar);
        if (!path.getFileSystem(conf).isFile(path)) {
          throw new IllegalArgumentException("Failed to find aux path " + auxJar);
        }
      }
    }
  }

  private void initialize(String[] initFiles, boolean inheritToClient) throws Exception {
    HiveSession serverSession = cliService.openServerSession(inheritToClient);
    if (initFiles == null || initFiles.length == 0) {
      return;
    }
    SessionHandle session = serverSession.getSessionHandle();
    for (String initFile : initFiles) {
      LOG.info("Loading : " + initFile);
      for (String query : HiveServer.loadScript(getHiveConf(), initFile)) {
        LOG.info("-- Executing : " + query);
        OperationHandle operation = cliService.executeStatement(session, query, null);
        if (operation.hasResultSet()) {
          TableSchema schema = cliService.getResultSetMetadata(operation);
          RowSet rowSet = cliService.fetchResults(operation);
          for (Object[] next : rowSet) {
            List<ColumnDescriptor> descs = schema.getColumnDescriptors();
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < descs.size(); i++) {
              Object eval = RowSet.evaluate(descs.get(i), next[i]);
              if (builder.length() > 0) {
                builder.append(", ");
              }
              builder.append(eval);
            }
            LOG.info("-- Executed : " + builder.toString());
          }
        }
        cliService.closeOperation(operation);
      }
    }
  }
}
