package org.apache.hadoop.yarn.api.records;

public interface ContainerStatus {
  ContainerId getContainerId();
  ContainerState getState();
  String getExitStatus();
  String getDiagnostics();
  
  void setContainerId(ContainerId containerId);
  void setState(ContainerState state);
  void setExitStatus(String exitStatus);
  void setDiagnostics(String diagnostics);
}
