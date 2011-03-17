/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.Resource;

public class ContainerTokenIdentifier extends TokenIdentifier {

  private static Log LOG = LogFactory
      .getLog(ContainerTokenIdentifier.class);

  public static final Text KIND = new Text("ContainerToken");

  private ContainerID containerID;
  private String nmHostName;
  private Resource resource;

  public ContainerTokenIdentifier(ContainerID containerID, String hostName, Resource r) {
    this.containerID = containerID;
    this.nmHostName = hostName;
    this.resource = r;
  }

  public ContainerTokenIdentifier() {
  }

  public ContainerID getContainerID() {
    return containerID;
  }

  public String getNmHostName() {
    return nmHostName;
  }

  public Resource getResource() {
    return resource;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    LOG.info("Writing ContainerTokenIdentifier to RPC layer");
    out.writeInt(this.containerID.appID.id);
    out.writeInt(this.containerID.id);
    out.writeUTF(this.nmHostName);
    out.writeInt(this.resource.memory); // TODO: more resources.
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.containerID = new ContainerID();
    this.containerID.appID = new ApplicationID();
    this.containerID.appID.id = in.readInt();
    this.containerID.id = in.readInt();
    this.nmHostName = in.readUTF();
    this.resource = new Resource();
    this.resource.memory = in.readInt(); // TODO: more resources.
  }

  @Override
  public Text getKind() {
    return this.KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    return UserGroupInformation.createRemoteUser(this.containerID.toString());
  }

}
