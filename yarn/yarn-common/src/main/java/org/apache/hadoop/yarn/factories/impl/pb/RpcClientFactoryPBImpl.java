package org.apache.hadoop.yarn.factories.impl.pb;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;

public class RpcClientFactoryPBImpl implements RpcClientFactory {

  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.client";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBClientImpl";
  
  private static final RpcClientFactoryPBImpl self = new RpcClientFactoryPBImpl();
  private Configuration localConf = new Configuration();
  private Map<Class<?>, Constructor<?>> cache = new HashMap<Class<?>, Constructor<?>>();
  
  public static RpcClientFactoryPBImpl get() {
    return RpcClientFactoryPBImpl.self;
  }
  
  private RpcClientFactoryPBImpl() {
  }
  
  public Object getClient(Class<?> protocol, long clientVersion, InetSocketAddress addr, Configuration conf) throws YarnException {
   
    Constructor<?> constructor = null;
    if (cache.get(protocol) == null) {
      Class<?> pbClazz = null;
      try {
        pbClazz = localConf.getClassByName(getPBImplClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnException("Failed to load class: ["
            + getPBImplClassName(protocol) + "]", e);
      }
      try {
        constructor = pbClazz.getConstructor(Long.TYPE, InetSocketAddress.class, Configuration.class);
        constructor.setAccessible(true);
        cache.put(protocol, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnException("Could not find constructor with params: " + Long.TYPE + ", " + InetSocketAddress.class + ", " + Configuration.class, e);
      }
    } else {
      constructor = cache.get(protocol);
    }

    try {
      Object retObject = constructor.newInstance(clientVersion, addr, conf);
      return retObject;
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    } catch (InstantiationException e) {
      throw new YarnException(e);
    }
  }
  
  
  
  private String getPBImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }
  
  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }
  
  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }
}