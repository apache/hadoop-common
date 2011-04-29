package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.NodeInfo;

public interface NodeManager extends NodeInfo {

  public static final String ANY = "*";

  void allocateContainer(ApplicationId applicationId,
      List<Container> containers);

  boolean releaseContainer(Container container);

  void updateHealthStatus(NodeHealthStatus healthStatus);

  NodeResponse statusUpdate(Map<String, List<Container>> containers);

  void notifyFinishedApplication(ApplicationId applicationId);

  Application getReservedApplication();
  
  void reserveResource(Application application, Priority priority, 
      Resource resource);
  
  void unreserveResource(Application application, Priority priority);
}
