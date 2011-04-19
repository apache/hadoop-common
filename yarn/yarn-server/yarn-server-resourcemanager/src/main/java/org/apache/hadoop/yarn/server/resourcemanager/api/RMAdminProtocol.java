package org.apache.hadoop.yarn.server.resourcemanager.api;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesResponse;

public interface RMAdminProtocol {
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request) 
  throws YarnRemoteException;
}
