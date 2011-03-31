package org.apache.hadoop.yarn.api;

import org.apache.hadoop.yarn.api.protocolrecords.CleanupContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CleanupContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;

public interface ContainerManager {
  StartContainerResponse startContainer(StartContainerRequest request) throws YarnRemoteException;
  StopContainerResponse stopContainer(StopContainerRequest request) throws YarnRemoteException;
  CleanupContainerResponse cleanupContainer(CleanupContainerRequest request) throws YarnRemoteException;
  GetContainerStatusResponse getContainerStatus(GetContainerStatusRequest request) throws YarnRemoteException;
}
