package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;

public interface DelegatableAuthorizationProvider extends HiveAuthorizationProvider {

  String AUTH_FOR = "authorizeFor";

  HiveAuthorizationProvider authorizeFor(ReadEntity readEntity);
}