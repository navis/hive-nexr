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

package org.apache.hadoop.hive.ql.session;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.util.DosToUnix;

/**
 * SessionState encapsulates common data associated with a session.
 *
 * Also provides support for a thread static session object that can be accessed
 * from any point in the code to interact with the user and to retrieve
 * configuration information
 */
public class SessionState {
  private static final Log LOG = LogFactory.getLog(SessionState.class);

  /**
   * current configuration.
   */
  protected HiveConf conf;

  /**
   * silent mode.
   */
  protected boolean isSilent;

  /**
   * verbose mode
   */
  protected boolean isVerbose;

  /*
   * HiveHistory Object
   */
  protected HiveHistory hiveHist;

  /**
   * Streams to read/write from.
   */
  public InputStream in;
  public PrintStream out;
  public PrintStream info;
  public PrintStream err;
  /**
   * Standard output from any child process(es).
   */
  public PrintStream childOut;
  /**
   * Error output from any child process(es).
   */
  public PrintStream childErr;

  /**
   * Temporary file name used to store results of non-Hive commands (e.g., set, dfs)
   * and HiveServer.fetch*() function will read results from this file
   */
  protected File tmpOutputFile;

  /**
   * type of the command.
   */
  private HiveOperation commandType;

  private HiveAuthorizationProvider authorizer;

  private HiveAuthenticationProvider authenticator;

  private CreateTableAutomaticGrant createTableGrants;

  private List<MapRedStats> lastMapRedStatsList;

  private Map<String, String> hiveVariables;

  // A mapping from a hadoop job ID to the stack traces collected from the map reduce task logs
  private Map<String, List<List<String>>> stackTraces;

  // This mapping collects all the configuration variables which have been set by the user
  // explicitely, either via SET in the CLI, the hiveconf option, or a System property.
  // It is a mapping from the variable name to its value.  Note that if a user repeatedly
  // changes the value of a variable, the corresponding change will be made in this mapping.
  private Map<String, String> overriddenConfigurations;

  private Map<String, List<String>> localMapRedErrors;

  /**
   * Lineage state.
   */
  private final LineageState ls;

  private String currentDB;

  private boolean initialized;

  /**
   * Get the lineage state stored in this session.
   *
   * @return LineageState
   */
  public LineageState getLineageState() {
    return ls;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public File getTmpOutputFile() {
    return tmpOutputFile;
  }

  public void setTmpOutputFile(File f) {
    tmpOutputFile = f;
  }

  public boolean getIsSilent() {
    if(conf != null) {
      return conf.getBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT);
    } else {
      return isSilent;
    }
  }

  public void setIsSilent(boolean isSilent) {
    if(conf != null) {
      conf.setBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT, isSilent);
    }
    this.isSilent = isSilent;
  }

  public boolean getIsVerbose() {
    return isVerbose;
  }

  public void setIsVerbose(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }

  public SessionState(HiveConf conf) {
    this.conf = conf;
    isSilent = conf.getBoolVar(HiveConf.ConfVars.HIVESESSIONSILENT);
    ls = new LineageState();
    overriddenConfigurations = new HashMap<String, String>();
    overriddenConfigurations.putAll(HiveConf.getConfSystemProperties());
    // if there isn't already a session name, go ahead and create it.
    if (StringUtils.isEmpty(conf.getVar(HiveConf.ConfVars.HIVESESSIONID))) {
      conf.setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
    }
  }

  private static final SimpleDateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");

  public void setCmd(String cmdString) {
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, cmdString);
  }

  public String getCmd() {
    return (conf.getVar(HiveConf.ConfVars.HIVEQUERYSTRING));
  }

  public String getQueryId() {
    return (conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
  }

  public Map<String, String> getHiveVariables() {
    if (hiveVariables == null) {
      hiveVariables = new HashMap<String, String>();
    }
    return hiveVariables;
  }

  public void setHiveVariables(Map<String, String> hiveVariables) {
    this.hiveVariables = hiveVariables;
  }

  public String getSessionId() {
    return (conf.getVar(HiveConf.ConfVars.HIVESESSIONID));
  }

  /**
   * Singleton Session object per thread.
   *
   **/
  private static ThreadLocal<SessionState> tss = new ThreadLocal<SessionState>();

  /**
   * start a new session and set it to current session.
   */
  public static SessionState start(HiveConf conf) {
    SessionState ss = new SessionState(conf);
    return start(ss);
  }

  /**
   * set current session to existing session object if a thread is running
   * multiple sessions - it must call this method with the new session object
   * when switching from one session to another.
   * @throws HiveException
   */
  public static SessionState start(SessionState startSs) {

    attachSession(startSs);

    try {
      startSs.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return startSs;
  }

  public static void attachSession(SessionState startSs) {
    tss.set(startSs);
  }

  public static void detachSession() {
    tss.remove();
  }

  private void start() throws Exception {

    Thread.currentThread().setContextClassLoader(conf.getClassLoader());

    if (!initialized) {
      String sessionID = makeSessionId();
      conf.setVar(HiveConf.ConfVars.HIVESESSIONID, sessionID);

      hiveHist = new HiveHistory(this);

      // per-session temp file containing results to be sent from HiveServer to HiveClient
      File tmpDir = new File(
          HiveConf.getVar(conf, HiveConf.ConfVars.HIVEHISTORYFILELOC));

      File tmpFile = File.createTempFile(sessionID, ".pipeout", tmpDir);
      tmpFile.deleteOnExit();
      setTmpOutputFile(tmpFile);

      Hive hive = Hive.get(conf);
      hive.setCurrentDatabase(currentDB);

      authenticator = HiveUtils.getAuthenticator(
          conf,HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER);
      authorizer = HiveUtils.getAuthorizeProviderManager(
          conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
          authenticator);
      createTableGrants = CreateTableAutomaticGrant.create(conf);

      initialized = true;
    }

    if (Thread.interrupted()) {
      getConsole().printInfo("Worker thread " + Thread.currentThread() +
          " is in interrupted state.. flag is cleaned-up");
    }
  }

  /**
   * get the current session.
   */
  public static SessionState get() {
    return tss.get();
  }

  /**
   * get hiveHitsory object which does structured logging.
   *
   * @return The hive history object
   */
  public HiveHistory getHiveHistory() {
    return hiveHist;
  }

  /**
   * Create a session ID. Looks like:
   *   $user_$pid@$host_$date
   * @return the unique string
   */
  private static String makeSessionId() {
    String userid = System.getProperty("user.name");
    return userid + "_" + ManagementFactory.getRuntimeMXBean().getName() + "_"
        + DATE_FORMAT.format(new Date());
  }

  public String getCurrentDB() {
    return currentDB;
  }

  public void setCurrentDB(String currentDB) {
    this.currentDB = currentDB;
  }

  public void destroy() {
    if (authenticator != null) {
      try {
        authenticator.destroy();
      } catch (HiveException e) {
        getConsole().printInfo("Failed to destroy authenticator by exception: " + e);
      }
    }
    if (tmpOutputFile != null) {
      try {
        tmpOutputFile.delete();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  /**
   * This class provides helper routines to emit informational and error
   * messages to the user and log4j files while obeying the current session's
   * verbosity levels.
   *
   * NEVER write directly to the SessionStates standard output other than to
   * emit result data DO use printInfo and printError provided by LogHelper to
   * emit non result data strings.
   *
   * It is perfectly acceptable to have global static LogHelper objects (for
   * example - once per module) LogHelper always emits info/error to current
   * session as required.
   */
  public static class LogHelper {

    protected Log LOG;
    protected boolean isSilent;

    public LogHelper(Log LOG) {
      this(LOG, false);
    }

    public LogHelper(Log LOG, boolean isSilent) {
      this.LOG = LOG;
      this.isSilent = isSilent;
    }

    public PrintStream getOutStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.out != null)) ? ss.out : System.out;
    }

    public PrintStream getInfoStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.info != null)) ? ss.info : getErrStream();
    }

    public PrintStream getErrStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.err != null)) ? ss.err : System.err;
    }

    public PrintStream getChildOutStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.childOut != null)) ? ss.childOut : System.out;
    }

    public PrintStream getChildErrStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.childErr != null)) ? ss.childErr : System.err;
    }

    public boolean getIsSilent() {
      SessionState ss = SessionState.get();
      // use the session or the one supplied in constructor
      return (ss != null) ? ss.getIsSilent() : isSilent;
    }

    public void printInfo(String info) {
      printInfo(info, null);
    }

    public void printInfo(String info, String detail) {
      if (!getIsSilent()) {
        getInfoStream().println(info);
      }
      LOG.info(info + StringUtils.defaultString(detail));
    }

    public void printError(String error) {
      printError(error, null);
    }

    public void printError(String error, String detail) {
      getErrStream().println(error);
      LOG.error(error + StringUtils.defaultString(detail));
    }
  }

  private static LogHelper _console;

  /**
   * initialize or retrieve console object for SessionState.
   */
  public static LogHelper getConsole() {
    if (_console == null) {
      Log LOG = LogFactory.getLog("SessionState");
      _console = new LogHelper(LOG);
    }
    return _console;
  }

  private static String validateFile(Map<String, String> curFiles, String newFile) {
    SessionState ss = SessionState.get();
    LogHelper console = getConsole();
    Configuration conf = (ss == null) ? new Configuration() : ss.getConf();

    try {
      if (Utilities.realFile(newFile, conf) != null) {
        return newFile;
      } else {
        console.printError(newFile + " does not exist");
        return null;
      }
    } catch (IOException e) {
      console.printError("Unable to validate " + newFile + "\nException: "
          + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return null;
    }
  }

  public static boolean registerJar(String newJar) {
    LogHelper console = getConsole();
    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      ClassLoader newLoader = Utilities.addToClassPath(loader, StringUtils.split(newJar, ","));
      Thread.currentThread().setContextClassLoader(newLoader);
      SessionState.get().getConf().setClassLoader(newLoader);
      console.printInfo("Added " + newJar + " to class path");
      return true;
    } catch (Exception e) {
      console.printError("Unable to register " + newJar + "\nException: "
          + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return false;
    }
  }

  public static boolean unregisterJar(String jarsToUnregister) {
    LogHelper console = getConsole();
    try {
      Utilities.removeFromClassPath(StringUtils.split(jarsToUnregister, ","));
      console.printInfo("Deleted " + jarsToUnregister + " from class path");
      return true;
    } catch (Exception e) {
      console.printError("Unable to unregister " + jarsToUnregister
          + "\nException: " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return false;
    }
  }

  /**
   * ResourceHook.
   *
   */
  public static interface ResourceHook {

    String preHook(Map<String, String> cur, String localized);

    boolean postHook(Map<String, String> cur, String localized);
  }

  /**
   * ResourceType.
   *
   */
  public static enum ResourceType {
    FILE(new ResourceHook() {
      public String preHook(Map<String, String> cur, String localized) {
        return validateFile(cur, localized);
      }

      public boolean postHook(Map<String, String> cur, String localized) {
        return true;
      }
    }),

    JAR(new ResourceHook() {
      public String preHook(Map<String, String> cur, String localized) {
        String newJar = validateFile(cur, localized);
        if (newJar != null) {
          return registerJar(newJar) ? newJar : null;
        } else {
          return null;
        }
      }

      public boolean postHook(Map<String, String> cur, String localized) {
        return unregisterJar(localized);
      }
    }),

    ARCHIVE(new ResourceHook() {
      public String preHook(Map<String, String> cur, String localized) {
        return validateFile(cur, localized);
      }

      public boolean postHook(Map<String, String> cur, String localized) {
        return true;
      }
    });

    public ResourceHook hook;

    ResourceType(ResourceHook hook) {
      this.hook = hook;
    }
  };

  public static ResourceType find_resource_type(String s) {

    s = s.trim().toUpperCase();

    try {
      return ResourceType.valueOf(s);
    } catch (IllegalArgumentException e) {
    }

    // try singular
    if (s.endsWith("S")) {
      s = s.substring(0, s.length() - 1);
    } else {
      return null;
    }

    try {
      return ResourceType.valueOf(s);
    } catch (IllegalArgumentException e) {
    }
    return null;
  }

  private final Map<ResourceType, Map<String, String>> resource_map =
    new HashMap<ResourceType, Map<String, String>>();

  public Map<ResourceType, Map<String, String>> getResourceMap() {
    return resource_map;
  }

  public void addResourceMap(Map<ResourceType, Map<String, String>> resource_map) {
    for (Map.Entry<ResourceType, Map<String, String>> entry : resource_map.entrySet()) {
      getResourceMap(entry.getKey()).putAll(entry.getValue());
    }
  }

  public String add_resource(ResourceType t, String value) {
    // By default don't convert to unix
    return add_resource(t, value, false);
  }

  public String add_resource(ResourceType t, String value, boolean convertToUnix) {
    Map<String, String> resourceMap = getResourceMap(t);
    String converted = resourceMap.get(value);
    if (converted != null) {
      return converted;
    }

    try {
      converted = downloadResource(value, convertToUnix);
    } catch (Exception e) {
      getConsole().printError(e.getMessage());
      return null;
    }

    if (t.hook != null) {
      converted = t.hook.preHook(resourceMap, converted);
      if (converted == null) {
        return null;
      }
    }
    getConsole().printInfo("Added resource: " + value + ":" + converted);
    resourceMap.put(value, converted);

    return converted;
  }

  public void add_builtin_resource(ResourceType t, String value) {
    getResourceMap(t).put(value, value);
  }

  private Map<String, String> getResourceMap(ResourceType t) {
    Map<String, String> result = resource_map.get(t);
    if (result == null) {
      result = new HashMap<String, String>();
      resource_map.put(t, result);
    }
    return result;
  }

  /**
   * Returns  true if it is from any external File Systems except local
   */
  public static boolean canDownloadResource(String value) {
    // Allow to download resources from any external FileSystem.
    // And no need to download if it already exists on local file system.
    String scheme = new Path(value).toUri().getScheme();
    return (scheme != null) && !scheme.equalsIgnoreCase("file");
  }

  private String downloadResource(String value, boolean convertToUnix) {
    if (canDownloadResource(value)) {
      getConsole().printInfo("converting to local " + value);
      File resourceDir = new File(getConf().getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
      String destinationName = new Path(value).getName();
      File destinationFile = new File(resourceDir, destinationName);
      if (resourceDir.exists() && ! resourceDir.isDirectory()) {
        throw new RuntimeException("The resource directory is not a directory, resourceDir is set to" + resourceDir);
      }
      if (!resourceDir.exists() && !resourceDir.mkdirs()) {
        throw new RuntimeException("Couldn't create directory " + resourceDir);
      }
      try {
        FileSystem fs = FileSystem.get(new URI(value), conf);
        fs.copyToLocalFile(new Path(value), new Path(destinationFile.getCanonicalPath()));
        value = destinationFile.getCanonicalPath();
        if (convertToUnix && DosToUnix.isWindowsScript(destinationFile)) {
          try {
            DosToUnix.convertWindowsScriptToUnix(destinationFile);
          } catch (Exception e) {
            throw new RuntimeException("Caught exception while converting file " +
                destinationFile + " to unix line endings", e);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to read external resource " + value, e);
      }
    }
    return value;
  }

  public boolean delete_resource(ResourceType t, String value) {
    Map<String, String> resources = resource_map.get(t);
    if (resources == null) {
      return false;
    }
    String localized = resources.remove(value);
    if (localized != null && t.hook != null) {
      if (!t.hook.postHook(resources, localized)) {
        return false;
      }
    }
    return localized != null;
  }

  public Set<String> list_resource(ResourceType t, List<String> filter) {
    if (resource_map.get(t) == null) {
      return null;
    }
    Map<String, String> orig = resource_map.get(t);
    if (orig == null || orig.isEmpty()) {
      return null;
    }
    if (filter == null) {
      return new HashSet<String>(orig.values());
    }
    Set<String> fnl = new HashSet<String>();
    for (String one : orig.values()) {
      if (filter.contains(one)) {
        fnl.add(one);
      }
    }
    return fnl;
  }

  public void delete_resource(ResourceType t) {
    Map<String, String> resources = resource_map.get(t);
    if (resources != null) {
      for (String value : resources.keySet()) {
        delete_resource(t, value);
      }
      resource_map.remove(t);
    }
  }

  public String getCommandType() {
    if (commandType == null) {
      return null;
    }
    return commandType.getOperationName();
  }

  public HiveOperation getHiveOperation() {
    return commandType;
  }

  public void setCommandType(HiveOperation commandType) {
    this.commandType = commandType;
  }

  public HiveAuthorizationProvider getAuthorizer() {
    return authorizer;
  }

  public void setAuthorizer(HiveAuthorizationProvider authorizer) {
    this.authorizer = authorizer;
  }

  public HiveAuthenticationProvider getAuthenticator() {
    return authenticator;
  }

  public void setAuthenticator(HiveAuthenticationProvider authenticator) {
    this.authenticator = authenticator;
  }

  public CreateTableAutomaticGrant getCreateTableGrants() {
    return createTableGrants;
  }

  public void setCreateTableGrants(CreateTableAutomaticGrant createTableGrants) {
    this.createTableGrants = createTableGrants;
  }

  public List<MapRedStats> getLastMapRedStatsList() {
    return lastMapRedStatsList;
  }

  public void setLastMapRedStatsList(List<MapRedStats> lastMapRedStatsList) {
    this.lastMapRedStatsList = lastMapRedStatsList;
  }

  public void setStackTraces(Map<String, List<List<String>>> stackTraces) {
    this.stackTraces = stackTraces;
  }

  public Map<String, List<List<String>>> getStackTraces() {
    return stackTraces;
  }

  public Map<String, String> getOverriddenConfigurations() {
    if (overriddenConfigurations == null) {
      overriddenConfigurations = new HashMap<String, String>();
    }
    return overriddenConfigurations;
  }

  public void setOverriddenConfigurations(Map<String, String> overriddenConfigurations) {
    this.overriddenConfigurations = overriddenConfigurations;
  }

  public Map<String, List<String>> getLocalMapRedErrors() {
    return localMapRedErrors;
  }

  public void addLocalMapRedErrors(String id, List<String> localMapRedErrors) {
    if (!this.localMapRedErrors.containsKey(id)) {
      this.localMapRedErrors.put(id, new ArrayList<String>());
    }

    this.localMapRedErrors.get(id).addAll(localMapRedErrors);
  }

  public void setLocalMapRedErrors(Map<String, List<String>> localMapRedErrors) {
    this.localMapRedErrors = localMapRedErrors;
  }

  public void close() throws IOException {
    File resourceDir =
      new File(getConf().getVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
    LOG.debug("Removing resource dir " + resourceDir);
    try {
      if (resourceDir.exists()) {
        FileUtils.deleteDirectory(resourceDir);
      }
    } catch (IOException e) {
      LOG.info("Error removing session resource dir " + resourceDir, e);
    } finally {
      detachSession();
      clear();
    }
  }

  private void clear() {
    ls.clear();
    authorizer = null;
    authenticator = null;
    lastMapRedStatsList = null;
    hiveVariables = null;
    stackTraces = null;
    overriddenConfigurations = null;
    localMapRedErrors = null;
  }
}
