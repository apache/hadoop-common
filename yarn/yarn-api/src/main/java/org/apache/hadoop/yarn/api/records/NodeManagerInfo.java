package org.apache.hadoop.yarn.api.records;

public interface NodeManagerInfo {
  String getNodeName();
  void setNodeName(String nodeName);
  String getRackName();
  void setRackName(String rackName);
  Resource getUsed();        
  void setUsed(Resource used);
  Resource getCapability();
  void setCapability(Resource capability);
  int getNumContainers();
  void setNumContainers(int numContainers);
}
