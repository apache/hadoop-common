package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface ContainerLaunchContext {
  public abstract ContainerId getContainerId();
  public abstract String getUser();
  public abstract Resource getResource();
  
  public abstract Map<String, LocalResource> getAllLocalResources();
  public abstract LocalResource getLocalResource(String key);
  
  
  public abstract ByteBuffer getContainerTokens();
  
  public abstract Map<String, ByteBuffer> getAllServiceData();
  public abstract ByteBuffer getServiceData(String key);
  
  public abstract Map<String, String> getAllEnv();
  public abstract String getEnv(String key);
  
  public abstract List<String> getCommandList();
  public abstract String getCommand(int index);
  public abstract int getCommandCount();
  
  public abstract void setContainerId(ContainerId containerId);
  public abstract void setUser(String user);
  public abstract void setResource(Resource resource);
  
  public abstract void addAllLocalResources(Map<String, LocalResource> localResources);
  public abstract void setLocalResource(String key, LocalResource value);
  public abstract void removeLocalResource(String key);
  public abstract void clearLocalResources();
  
  public abstract void setContainerTokens(ByteBuffer containerToken);
  
  public abstract void addAllServiceData(Map<String, ByteBuffer> serviceData);
  public abstract void setServiceData(String key, ByteBuffer value);
  public abstract void removeServiceData(String key);
  public abstract void clearServiceData();
  
  public abstract void addAllEnv(Map<String, String> env);
  public abstract void setEnv(String key, String value);
  public abstract void removeEnv(String key);
  public abstract void clearEnv();
  
  public abstract void addAllCommands(List<String> commands);
  public abstract void addCommand(String command);
  public abstract void removeCommand(int index);
  public abstract void clearCommands();
}
