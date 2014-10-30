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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * CommandProcessorFactory.
 *
 */
public final class CommandProcessorFactory {

  private static final Log LOG = LogFactory.getLog(CommandProcessorFactory.class.getName());

  private static final List<HiveCommand> COMMANDS;
  private static final Set<String> WHITE_LISTS;
  private static final boolean HIVE_IN_TEST;

  static {
    List<HiveCommand> commands = new ArrayList<HiveCommand>();
    // hive builtin first
    commands.add(new NativeCommands.ADD());
    commands.add(new NativeCommands.COMPILE());
    commands.add(new NativeCommands.DELETE());
    commands.add(new NativeCommands.DFS());
    commands.add(new NativeCommands.LIST());
    commands.add(new NativeCommands.RELOAD());
    commands.add(new NativeCommands.RESET());
    commands.add(new NativeCommands.SET());
    commands.add(new NativeCommands.CRYPTO());

    HiveConf hiveConf = new HiveConf(CommandProcessorFactory.class);
    String var = hiveConf.getVar(HiveConf.ConfVars.HIVE_COMMAND_PROCESSORS);
    for (String className : StringUtils.getTrimmedStrings(var)) {
      if (!className.isEmpty()) {
        try {
          commands.add((HiveCommand) ReflectionUtils.newInstance(
              Class.forName(className), hiveConf));
        } catch (Exception e) {
          LOG.warn("Failed to register hive command " + className, e);
        }
      }
    }
    COMMANDS = Collections.unmodifiableList(commands);

    Set<String> whiteLists = new HashSet<String>();
    var = hiveConf.getVar(HiveConf.ConfVars.HIVE_SECURITY_COMMAND_WHITELIST);
    for (String availableCommand : StringUtils.getTrimmedStrings(var)) {
      whiteLists.add(availableCommand.toLowerCase());
    }
    WHITE_LISTS = Collections.unmodifiableSet(whiteLists);
    HIVE_IN_TEST = HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST);
  }

  private CommandProcessorFactory() {
    // prevent instantiation
  }

  @VisibleForTesting
  static List<HiveCommand> getCommands() {
    return COMMANDS;
  }

  public static CommandProcessor get(String cmd) throws SQLException {
    return get(cmd.trim().split("\\s+"));
  }

  public static CommandProcessor get(String[] cmd) throws SQLException {
    CommandProcessor result = getForHiveCommand(cmd);
    if (result == null) {
      result = new Driver();
    }
    return result;
  }

  public static CommandProcessor getForHiveCommand(String[] cmd) throws SQLException {
    HiveCommand hiveCommand = cmd == null ? null : findCommand(cmd);
    if (hiveCommand == null) {
      return null;
    }
    if (!WHITE_LISTS.contains(hiveCommand.getName().toLowerCase())) {
      throw new SQLException("Insufficient privileges to execute " + cmd[0], "42000");
    }
    try {
      return hiveCommand.getProcessor(cmd);
    } catch (HiveException e) {
      throw new SQLException("Fail to start the command processor due to the exception: ", e);
    }
  }

  public static HiveCommand findCommand(String[] cmd) {
    for (HiveCommand command : COMMANDS) {
      if (command.accepts(cmd) && (!command.isInternal() || HIVE_IN_TEST)) {
        return command; 
      }
    }
    return null;
  }
}
