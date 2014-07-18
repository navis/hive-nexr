package org.apache.hadoop.hive.ql.security.authorization;

public interface DelegatableAuthorizationProvider extends HiveAuthorizationProvider {

  String AUTH_WITH = "authorizeWith";

  HiveAuthorizationProvider authorizeWith(String userName);
}
