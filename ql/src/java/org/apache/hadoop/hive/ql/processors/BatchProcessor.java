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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.ArrayList;
import java.util.List;

public class BatchProcessor extends SimpleProcessor {

  private List<CommandProcessor> processors = new ArrayList<CommandProcessor>();

  @Override
  public CommandProcessorResponse prepare(String command) {
    CommandProcessorResponse response = null;

    StringBuilder commands = new StringBuilder();
    for (String oneCmd : command.split(";")) {

      if (StringUtils.endsWith(oneCmd, "\\")) {
        commands.append(StringUtils.chop(oneCmd)).append(';');
        continue;
      } else {
        commands.append(oneCmd);
      }
      String statement = commands.toString();
      if (StringUtils.isBlank(statement)) {
        continue;
      }

      int index = statement.indexOf("=");
      if (index > 0 && isValid(statement.substring(index + 1))) {
        statement = "CREATE TEMPORARY VIEW " +
            statement.substring(0, index).trim() + " AS " + statement.substring(index + 1);
      }
      try {
        CommandProcessor processor = CommandProcessorFactory.get(statement);
        if (processor instanceof BatchProcessor) {
          processor = new Driver();
        }
        response = processor.prepare(statement);
        if (response.getResponseCode() != 0) {
          return response;
        }
        processors.add(processor);
      } catch (Exception e) {
        return CommandProcessorResponse.create(e);
      }
    }
    return response != null ? response : new CommandProcessorResponse(0);
  }

  @Override
  public CommandProcessorResponse run() throws CommandNeedRetryException {
    CommandProcessorResponse response = null;
    for (CommandProcessor processor : processors) {
      response = processor.run();
      if (response.getResponseCode() != 0) {
        return response;
      }
    }
    return response != null ? response : new CommandProcessorResponse(0);
  }

  private boolean isValid(String command) {
    try {
      Context ctx = new Context(conf);
      ctx.setCmd(command);
      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command, ctx);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  @Override
  public int close() {
    for (CommandProcessor processor : processors) {
      processor.close();
    }
    return super.close();
  }
}
