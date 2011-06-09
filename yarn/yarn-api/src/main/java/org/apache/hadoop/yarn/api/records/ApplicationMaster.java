package org.apache.hadoop.yarn.api.records;


//TODO: Split separate object for register, deregister and in-RM use.
public interface ApplicationMaster {
  ApplicationId getApplicationId();
  String getHost();
  int getRpcPort();
  String getTrackingUrl();
  ApplicationStatus getStatus();
  ApplicationState getState();
  String getClientToken();
  int getAMFailCount();
  int getContainerCount();
  String getDiagnostics();
  void setApplicationId(ApplicationId appId);
  void setHost(String host);
  void setRpcPort(int rpcPort);
  void setTrackingUrl(String url);
  void setStatus(ApplicationStatus status);
  void setState(ApplicationState state);
  void setClientToken(String clientToken);
  void setAMFailCount(int amFailCount);
  void setContainerCount(int containerCount);
  void setDiagnostics(String diagnostics);
}
