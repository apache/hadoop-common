package org.apache.hadoop.yarn.factories.impl.pb;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.exceptions.impl.pb.YarnRemoteExceptionPBImpl;
import org.apache.hadoop.yarn.factories.YarnRemoteExceptionFactory;

public class YarnRemoteExceptionFactoryPBImpl implements
    YarnRemoteExceptionFactory {

  private static final YarnRemoteExceptionFactory self = new YarnRemoteExceptionFactoryPBImpl();

  private YarnRemoteExceptionFactoryPBImpl() {
  }

  public static YarnRemoteExceptionFactory get() {
    return self;
  }

  @Override
  public YarnRemoteException createYarnRemoteException(String message) {
    return new YarnRemoteExceptionPBImpl(message);
  }

  @Override
  public YarnRemoteException createYarnRemoteException(String message,
      Throwable t) {
    return new YarnRemoteExceptionPBImpl(message, t);
  }

  @Override
  public YarnRemoteException createYarnRemoteException(Throwable t) {
    return new YarnRemoteExceptionPBImpl(t);
  }
}
