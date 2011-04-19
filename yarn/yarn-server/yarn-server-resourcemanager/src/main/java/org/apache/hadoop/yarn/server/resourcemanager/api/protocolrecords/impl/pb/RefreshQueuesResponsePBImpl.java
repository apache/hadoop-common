package org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.records.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RefreshQueuesResponseProto;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesResponse;

public class RefreshQueuesResponsePBImpl extends ProtoBase<RefreshQueuesResponseProto>
implements RefreshQueuesResponse {

  RefreshQueuesResponseProto proto = RefreshQueuesResponseProto.getDefaultInstance();
  RefreshQueuesResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  public RefreshQueuesResponsePBImpl() {
    builder = RefreshQueuesResponseProto.newBuilder();
  }

  public RefreshQueuesResponsePBImpl(RefreshQueuesResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public RefreshQueuesResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
}
