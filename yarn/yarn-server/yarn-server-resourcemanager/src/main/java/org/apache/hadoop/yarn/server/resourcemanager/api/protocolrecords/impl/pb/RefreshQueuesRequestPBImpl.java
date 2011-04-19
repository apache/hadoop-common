package org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesRequestProto;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;

public class RefreshQueuesRequestPBImpl extends ProtoBase<RefreshQueuesRequestProto>
implements RefreshQueuesRequest {

  RefreshQueuesRequestProto proto = RefreshQueuesRequestProto.getDefaultInstance();
  RefreshQueuesRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  public RefreshQueuesRequestPBImpl() {
    builder = RefreshQueuesRequestProto.newBuilder();
  }

  public RefreshQueuesRequestPBImpl(RefreshQueuesRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RefreshQueuesRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
}
