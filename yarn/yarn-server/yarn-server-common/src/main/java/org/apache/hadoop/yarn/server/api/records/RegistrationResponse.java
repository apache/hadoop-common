package org.apache.hadoop.yarn.server.api.records;

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.api.records.NodeId;

public interface RegistrationResponse {
  public abstract NodeId getNodeId();
  public abstract ByteBuffer getSecretKey();
  
  public abstract void setNodeId(NodeId nodeId);
  public abstract void setSecretKey(ByteBuffer secretKey);
}
