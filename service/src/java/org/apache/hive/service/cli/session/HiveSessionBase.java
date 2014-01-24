package org.apache.hive.service.cli.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.OperationManager;

public interface HiveSessionBase {

  /**
   * Set the session manager for the session
   * @param sessionManager
   */
  public void setSessionManager(SessionManager sessionManager);

  /**
   * Set operation manager for the session
   * @param operationManager
   */
  public void setOperationManager(OperationManager operationManager);

  public SessionHandle getSessionHandle();

  public String getUsername();

  public String getPassword();

  public HiveConf getHiveConf();

  public SessionState getSessionState();

  public String getUserName();

  public void setUserName(String userName);

  public long getStartTime();
}
