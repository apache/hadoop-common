package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;

import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.Container;

/**
 * The class that encapsulates response from clusterinfo for 
 * updates from the node managers.
 */
public class NodeResponse {
  private final List<Container> completed;
  private final List<Container> toCleanUp;
  private final List<ApplicationID> finishedApplications;
  
  public NodeResponse(List<ApplicationID> finishedApplications,
      List<Container> completed, List<Container> toKill) {
    this.finishedApplications = finishedApplications;
    this.completed = completed;
    this.toCleanUp = toKill;
  }
  public List<ApplicationID> getFinishedApplications() {
    return this.finishedApplications;
  }
  public List<Container> getCompletedContainers() {
    return this.completed;
  }
  public List<Container> getContainersToCleanUp() {
    return this.toCleanUp;
  }
}