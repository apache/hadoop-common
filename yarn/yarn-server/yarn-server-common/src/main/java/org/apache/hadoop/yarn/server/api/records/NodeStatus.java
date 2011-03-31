package org.apache.hadoop.yarn.server.api.records;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;


public interface NodeStatus {
  
  public abstract NodeId getNodeId();
  public abstract int getResponseId();
  public abstract long getLastSeen();
  
  public abstract Map<String, List<Container>> getAllContainers();
  public abstract List<Container> getContainers(String key);
  
  public abstract boolean getIsNodeHealthy();
  public abstract String getHealthReport();
  public abstract long getLastHealthReport();
  
  
  public abstract void setNodeId(NodeId nodeId);
  public abstract void setResponseId(int responseId);
  public abstract void setLastSeen(long lastSeen);
  
  public abstract void addAllContainers(Map<String, List<Container>> containers);
  public abstract void setContainers(String key, List<Container> containers);
  public abstract void removeContainers(String key);
  public abstract void clearContainers();
  
  public abstract void setIsNodeHealthy(boolean isNodeHealthy);
  public abstract void setHealthReport(String healthReport);
  public abstract void setLastHealthReport(long lastHealthReport);
}
