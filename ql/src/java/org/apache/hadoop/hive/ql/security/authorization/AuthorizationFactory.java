package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthorizationFactory {

  public static DelegatableAuthorizationProvider create(HiveAuthorizationProvider delegated) {
    return create(delegated, new DefaultAuthorizationExceptionHandler());
  }

  public static DelegatableAuthorizationProvider create(final HiveAuthorizationProvider delegated,
      final AuthorizationExceptionHandler handler) {

    final String user = delegated.getAuthenticator().getUserName();
    final Set<String> owners = new HashSet<String>();
    InvocationHandler invocation = new InvocationHandler() {
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals(DelegatableAuthorizationProvider.AUTH_FOR)) {
          getOwnersFrom((ReadEntity) args[0], owners, user);
          return proxy;
        }
        if (owners.isEmpty()) {
          invokeAuth(method, args);
          return null;
        }
        HiveAuthenticationProvider prev = delegated.getAuthenticator();
        try {
          for (String owner : owners) {
            delegated.setAuthenticator(new OwnerAuthenticationProvider(owner));
            invokeAuth(method, args);
          }
        } finally {
          owners.clear();
          delegated.setAuthenticator(prev);
        }
        return null;
      }

      private void invokeAuth(Method method, Object[] args) throws Throwable {
        try {
          method.invoke(delegated, args);
        } catch (InvocationTargetException e) {
          if (e.getTargetException() instanceof AuthorizationException) {
            handler.exception((AuthorizationException) e.getTargetException());
          }
        }
      }

      private void getOwnersFrom(ReadEntity readEntity, Set<String> owners, String user) {
        owners.clear();
        Set<ReadEntity> parents = readEntity.getParents();
        if (parents == null || parents.isEmpty()) {
          return;
        }
        for (ReadEntity parent : parents) {
          if (parent.getType() == Entity.Type.TABLE) {
            owners.add(parent.getT().getTTable().getOwner());
          } else if (parent.getType() == Entity.Type.TABLE) {
            owners.add(parent.getP().getTable().getTTable().getOwner());
          }
        }
        owners.remove(user);
      }
    };

    return (DelegatableAuthorizationProvider)Proxy.newProxyInstance(
        AuthorizationFactory.class.getClassLoader(),
        new Class[] {DelegatableAuthorizationProvider.class},
        invocation);
  }

  public static class OwnerAuthenticationProvider implements HiveAuthenticationProvider {

    private final String userName;

    private OwnerAuthenticationProvider(String userName) {
      this.userName = userName;
    }

    public String getUserName() {
      return userName;
    }

    public List<String> getGroupNames() {
      return null;
    }

    public void destroy() throws HiveException {
    }

    public void setConf(Configuration conf) {
    }

    public Configuration getConf() {
      return null;
    }
  }

  public static interface AuthorizationExceptionHandler {
    void exception(AuthorizationException exception) throws AuthorizationException;
  }

  public static class DefaultAuthorizationExceptionHandler
      implements AuthorizationExceptionHandler {
    public void exception(AuthorizationException exception) {
      throw exception;
    }
  }
}
