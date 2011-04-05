package org.apache.hadoop.yarn.server.api.records;

public interface NodeHealthStatus {

  boolean getIsNodeHealthy();

  String getHealthReport();

  long getLastHealthReportTime();

  void setIsNodeHealthy(boolean isNodeHealthy);

  void setHealthReport(String healthReport);

  void setLastHealthReportTime(long lastHealthReport);
}