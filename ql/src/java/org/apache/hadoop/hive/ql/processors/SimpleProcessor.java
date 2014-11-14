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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.parse.VariableSubstitution;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public abstract class SimpleProcessor implements CommandProcessor {

  protected HiveConf conf;
  protected SessionState session;

  protected String command;

  /**
   * For processors other than Hive queries (Driver), they output to session.out (a temp file)
   * first and the fetchOne/fetchN/fetchAll functions get the output from pipeIn.
   */
  private transient BufferedReader resultReader;
  private Object schema;

  @Override
  public void init(HiveConf conf, SessionState session) {
    this.conf = conf;
    this.session = session;
  }

  public CommandProcessorResponse run(String command) throws CommandNeedRetryException {
    CommandProcessorResponse response = prepare(command);
    if (response.getResponseCode() != 0) {
      return response;
    }
    return run();
  }

  @Override
  public CommandProcessorResponse prepare(String command) {
    this.command = new VariableSubstitution().substitute(conf, getCommandArgs(command));
    return createProcessorSuccessResponse();
  }

  public CommandProcessorResponse run() throws CommandNeedRetryException {
    return runCommand(command);
  }

  public boolean isFromFetchTask() {
    return false;
  }

  /**
   * Reads the temporary results for non-Hive (non-Driver) commands to the
   * resulting List of strings.
   * @param nLines number of lines read at once. If it is <= 0, then read all lines.
   */
  @Override
  public boolean getResults(List results, long nLines) throws IOException, CommandNeedRetryException {
    if (resultReader == null) {
      File tmp = session.getTmpOutputFile();
      try {
        resultReader = new BufferedReader(new FileReader(tmp));
      } catch (FileNotFoundException e) {
        session.LOG.error("File " + tmp + " not found. ", e);
        throw e;
      }
    }
    int current = results.size();
    for (int i = 0; i < nLines || nLines <= 0; ++i) {
      try {
        String line = resultReader.readLine();
        if (line == null) {
          // reached the end of the result file
          break;
        } else {
          results.add(line);
        }
      } catch (IOException e) {
        session.LOG.error("Reading temp results encountered an exception: ", e);
        throw e;
      }
    }
    return results.size() != current;
  }

  @Override
  public void resetFetch() {
    IOUtils.cleanup(session.LOG, resultReader);
    resultReader = null;
  }

  @Override
  public int close() {
    if (session.getTmpOutputFile() != null) {
      session.getTmpOutputFile().delete();
    }
    return 0;
  }

  protected String getCommandArgs(String statement) {
    String[] tokens = statement.split("\\s");
    return statement.substring(tokens[0].length()).trim();  // remove command part
  }

  protected CommandProcessorResponse createProcessorSuccessResponse() {
    return new CommandProcessorResponse(0, null, null, getSchema());
  }

  protected Schema getSchema() {
    return null;
  }

  protected CommandProcessorResponse runCommand(String command) throws CommandNeedRetryException {
    return new CommandProcessorResponse(0);
  }

}
