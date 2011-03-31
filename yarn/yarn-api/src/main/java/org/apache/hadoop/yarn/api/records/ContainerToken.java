package org.apache.hadoop.yarn.api.records;

import java.nio.ByteBuffer;

public interface ContainerToken {
  public abstract ByteBuffer getIdentifier();
  public abstract ByteBuffer getPassword();
  public abstract String getKind();
  public abstract String getService();
  
  public abstract void setIdentifier(ByteBuffer identifier);
  public abstract void setPassword(ByteBuffer password);
  public abstract void setKind(String kind);
  public abstract void setService(String service);

}
