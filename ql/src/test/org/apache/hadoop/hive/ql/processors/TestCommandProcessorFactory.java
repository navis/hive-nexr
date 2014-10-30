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

package org.apache.hadoop.hive.ql.processors;

import junit.framework.Assert;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

public class TestCommandProcessorFactory {

  private final String[] testOnlyCommands = new String[]{"crypto"};

  private HiveConf conf;

  @Before
  public void setUp() throws Exception {
    conf = new HiveConf();
  }

  @Test
  public void testInvalidCommands() throws Exception {
    Assert.assertNull("Null should have returned null", CommandProcessorFactory.getForHiveCommand(null));
    Assert.assertNull("Blank should have returned null", CommandProcessorFactory.getForHiveCommand(new String[]{" "}));
    Assert.assertNull("set role should have returned null", CommandProcessorFactory.getForHiveCommand(new String[]{"set role"}));
    Assert.assertNull("SQL should have returned null", CommandProcessorFactory.getForHiveCommand(new String[]{"SELECT * FROM TABLE"}));
  }

  @Test
  public void testAvailableCommands() throws Exception {
    for (HiveCommand command : CommandProcessorFactory.getCommands()) {
      String cmd = command.getName();
      String cmdInLowerCase = cmd.toLowerCase();
      Assert.assertNotNull("Cmd " + cmd + " not return null",
          CommandProcessorFactory.getForHiveCommand(new String[]{cmd}));
      Assert.assertNotNull("Cmd " + cmd + " not return null",
          CommandProcessorFactory.getForHiveCommand(new String[]{cmdInLowerCase}));
      
      Assert.assertNotNull("Cmd " + cmd + " not return null", CommandProcessorFactory.getForHiveCommand(new String[]{cmd}));
    }
    for (HiveCommand command : CommandProcessorFactory.getCommands()) {
      String cmd = command.getName();
      Assert.assertNotNull("Cmd " + cmd + " not return null", CommandProcessorFactory.getForHiveCommand(new String[]{cmd}));
    }
  }

  private void enableTestOnlyCmd(HiveConf conf){
    StringBuilder securityCMDs = new StringBuilder(conf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST));
    for(String c : testOnlyCommands){
      securityCMDs.append(",");
      securityCMDs.append(c);
    }
    conf.set(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST.toString(), securityCMDs.toString());
  }
}
