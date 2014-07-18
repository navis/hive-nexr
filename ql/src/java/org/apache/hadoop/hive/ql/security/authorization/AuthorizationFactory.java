package org.apache.hadoop.hive.ql.security.authorization;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;

public class AuthorizationFactory {

  public static DelegatableAuthorizationProvider create(HiveAuthorizationProvider delegated) {
    return create(delegated, new DefaultAuthorizationExceptionHandler());
  }

  public static DelegatableAuthorizationProvider create(final HiveAuthorizationProvider delegated,
      final AuthorizationExceptionHandler handler) {

    final String user = delegated.getAuthenticator().getUserName();

    InvocationHandler invocation = new InvocationHandler() {
      private String userName = null;
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals(DelegatableAuthorizationProvider.AUTH_WITH)) {
          userName = (String)args[0];
          return proxy;
        }
        if (userName == null) {
          return invokeAuth(method, args);
        }
        HiveAuthenticationProvider prev = delegated.getAuthenticator();
        try {
          delegated.setAuthenticator(new DelegatedUserProvider(userName));
          return invokeAuth(method, args);
        } finally {
          delegated.setAuthenticator(prev);
        }
      }

      private Object invokeAuth(Method method, Object[] args) throws Throwable {
        try {
          return method.invoke(delegated, args);
        } catch (InvocationTargetException e) {
          if (e.getTargetException() instanceof AuthorizationException) {
            handler.exception((AuthorizationException) e.getTargetException());
            return null;
          }
          throw e.getTargetException();
        }
      }
    };

    return (DelegatableAuthorizationProvider)Proxy.newProxyInstance(
        AuthorizationFactory.class.getClassLoader(),
        new Class[] {DelegatableAuthorizationProvider.class},
        invocation);
  }

  public static class DelegatedUserProvider implements HiveAuthenticationProvider {

    private final String userName;

    private DelegatedUserProvider(String userName) {
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
