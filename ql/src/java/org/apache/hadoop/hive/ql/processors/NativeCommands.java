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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

// hive builtin commands
public class NativeCommands {

  private abstract static class AbstractCommand implements HiveCommand {
    @Override
    public String getName() { return getClass().getSimpleName().toLowerCase(); }
    @Override
    public boolean isInternal() { return false; }
    @Override
    public boolean accepts(String[] command) {
      return !StringUtils.isEmpty(command[0]) && getName().equalsIgnoreCase(command[0]);
    }
  }

  public static class SET extends AbstractCommand {
    @Override
    public boolean accepts(String[] command) {
      // special handling for set role r1 statement
      return super.accepts(command) &&
          (command.length == 1 || !command[1].equalsIgnoreCase("role"));
    }
    @Override
    public CommandProcessor getProcessor(String[] command) { return new SetProcessor(); }
  }

  public static class RESET extends AbstractCommand {
    @Override
    public CommandProcessor getProcessor(String[] command) { return new ResetProcessor(); }
  }

  public static class DFS extends AbstractCommand {
    @Override
    public CommandProcessor getProcessor(String[] command) { return new DfsProcessor(); }
  }

  public static class ADD extends AbstractCommand {
    @Override
    public CommandProcessor getProcessor(String[] command) { return new AddResourceProcessor(); }
  }

  public static class LIST extends AbstractCommand {
    @Override
    public CommandProcessor getProcessor(String[] command) { return new ListResourceProcessor(); }
  }

  public static class RELOAD extends AbstractCommand {
    @Override
    public CommandProcessor getProcessor(String[] command) { return new ReloadProcessor(); }
  }

  public static class DELETE extends AbstractCommand {
    @Override
    public boolean accepts(String[] command) {
      //special handling for SQL "delete from <table> where..."
      return super.accepts(command) &&
          (command.length == 1 || !command[1].equalsIgnoreCase("from"));
    }
    @Override
    public CommandProcessor getProcessor(String[] command) { return new DeleteResourceProcessor(); }
  }

  public static class COMPILE extends AbstractCommand {
    @Override
    public CommandProcessor getProcessor(String[] command) { return new CompileProcessor(); }
  }

  public static class CRYPTO extends AbstractCommand {
    @Override
    public boolean isInternal() { return true; }
    @Override
    public CommandProcessor getProcessor(String[] command) throws HiveException { 
      return new CryptoProcessor(SessionState.get().getHdfsEncryptionShim()); 
    }
  }
}
