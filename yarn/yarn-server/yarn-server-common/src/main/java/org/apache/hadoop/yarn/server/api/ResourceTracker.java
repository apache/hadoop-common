package org.apache.hadoop.yarn.server.api;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;

public interface ResourceTracker {
  
  public RegisterNodeManagerResponse registerNodeManager(RegisterNodeManagerRequest request) throws YarnRemoteException;
  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request) throws YarnRemoteException;

}
