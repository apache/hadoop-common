package org.apache.hadoop.yarn.factories.impl.pb;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RpcServerFactory;
import org.apache.hadoop.yarn.ipc.ProtoOverHadoopRpcEngine;

import com.google.protobuf.BlockingService;

public class RpcServerFactoryPBImpl implements RpcServerFactory {

  private static final String PROTO_GEN_PACKAGE_NAME = "org.apache.hadoop.yarn.proto";
  private static final String PROTO_GEN_CLASS_SUFFIX = "Service";
  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.service";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBServiceImpl";
  
  private static final RpcServerFactoryPBImpl self = new RpcServerFactoryPBImpl();

  private Configuration localConf = new Configuration();
  private Map<Class<?>, Constructor<?>> serviceCache = new HashMap<Class<?>, Constructor<?>>();
  private Map<Class<?>, Method> protoCache = new HashMap<Class<?>, Method>();
  
  public static RpcServerFactoryPBImpl get() {
    return RpcServerFactoryPBImpl.self;
  }
  

  private RpcServerFactoryPBImpl() {
  }
  
  @Override
  public Server getServer(Class<?> protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager)
      throws YarnException {
    
    Constructor<?> constructor = null;
    if (serviceCache.get(protocol) == null) {
      Class<?> pbServiceImplClazz = null;
      try {
        pbServiceImplClazz = localConf
            .getClassByName(getPbServiceImplClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnException("Failed to load class: ["
            + getPbServiceImplClassName(protocol) + "]", e);
      }
      try {
        constructor = pbServiceImplClazz.getConstructor(protocol);
        constructor.setAccessible(true);
        serviceCache.put(protocol, constructor);
      } catch (NoSuchMethodException e) {
        throw new YarnException("Could not find constructor with params: "
            + Long.TYPE + ", " + InetSocketAddress.class + ", "
            + Configuration.class, e);
      }
    } else {
      constructor = serviceCache.get(protocol);
    }
    
    Object service = null;
    try {
      service = constructor.newInstance(instance);
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    } catch (InstantiationException e) {
      throw new YarnException(e);
    }

    Method method = null;
    if (protoCache.get(protocol) == null) {
      Class<?> protoClazz = null;
      try {
        protoClazz = localConf.getClassByName(getProtoClassName(protocol));
      } catch (ClassNotFoundException e) {
        throw new YarnException("Failed to load class: ["
            + getProtoClassName(protocol) + "]", e);
      }
      try {
        method = protoClazz.getMethod("newReflectiveBlockingService", service.getClass().getInterfaces()[0]);
        method.setAccessible(true);
        protoCache.put(protocol, method);
      } catch (NoSuchMethodException e) {
        throw new YarnException(e);
      }
    } else {
      method = protoCache.get(protocol);
    }
    
    try {
      return createServer(addr, conf, secretManager, (BlockingService)method.invoke(null, service));
    } catch (InvocationTargetException e) {
      throw new YarnException(e);
    } catch (IllegalAccessException e) {
      throw new YarnException(e);
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }
  
  private String getProtoClassName(Class<?> clazz) {
    String srcClassName = getClassName(clazz);
    return PROTO_GEN_PACKAGE_NAME + "." + srcClassName + "$" + srcClassName + PROTO_GEN_CLASS_SUFFIX;  
  }
  
  private String getPbServiceImplClassName(Class<?> clazz) {
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

  private Server createServer(InetSocketAddress addr, Configuration conf, SecretManager<? extends TokenIdentifier> secretManager, BlockingService blockingService) throws IOException {
    RPC.setProtocolEngine(conf, BlockingService.class, ProtoOverHadoopRpcEngine.class);
    Server server = RPC.getServer(BlockingService.class, blockingService, addr.getHostName(), addr.getPort(), 1, false, conf, secretManager);
    return server;
  }
}
