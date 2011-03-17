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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerManagerSecurityInfo;
import org.apache.hadoop.yarn.util.AvroUtil;

import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ContainerID;
import org.apache.hadoop.yarn.ContainerLaunchContext;
import org.apache.hadoop.yarn.ContainerManager;
import org.apache.hadoop.yarn.LocalResource;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.URL;
import org.apache.hadoop.yarn.YarnRemoteException;
import static org.apache.hadoop.yarn.LocalResourceType.*;
import static org.apache.hadoop.yarn.LocalResourceVisibility.*;

public class SyntheticContainerLaunch {

  static final long clusterTimeStamp = System.nanoTime();

  static ContainerLaunchContext getContainer(Configuration conf,
      int appId, int cId, String user, Path tokens)
      throws IOException, URISyntaxException {
    ContainerLaunchContext container = new ContainerLaunchContext();
    // id
    ApplicationID appID = new ApplicationID();
    appID.id = appId;
    appID.clusterTimeStamp = clusterTimeStamp;
    container.id = new ContainerID();
    container.id.appID = appID;
    container.id.id = cId;

    // user
    container.user = user;

    // Resource resource
    container.resource = new Resource();
    container.resource.memory = 1024;

    // union {null, map<LocalResource>} resources_todo;
    container.resources = new HashMap<CharSequence,LocalResource>();
    LocalResource resource = new LocalResource();
    resource.resource = AvroUtil.getYarnUrlFromPath(
        new Path("file:///home/chrisdo/work/hadoop/mapred/CHANGES.txt"));
    resource.size = -1;
    resource.timestamp = 1294684255000L;
    resource.type = FILE;
    resource.state = PRIVATE;
    container.resources.put("dingos", resource);

    //union {null, bytes} fsTokens_todo;
    Credentials creds = new Credentials();
    if (tokens != null) {
      creds.readTokenStorageFile(tokens, conf);
    }
    DataOutputBuffer buf = new DataOutputBuffer();
    creds.writeTokenStorageToStream(buf);
    container.containerTokens =
      ByteBuffer.wrap(buf.getData(), 0, buf.getLength());

    //union {null, map<bytes>} serviceData;
    container.serviceData = new HashMap<CharSequence,ByteBuffer>();

    // map<string> env;
    container.env = new HashMap<CharSequence,CharSequence>();
    container.env.put("MY_OUTPUT_FILE", "yak.txt");

    // array<string> command;
    container.command = new ArrayList<CharSequence>();
    container.command.add("cat");
    container.command.add("dingos");
    container.command.add(">");
    container.command.add("${MY_OUTPUT_FILE}");
    return container;
  }

  static ContainerManager getClient(Configuration conf, InetSocketAddress adr) {
    YarnRPC rpc = YarnRPC.create(conf);
    //conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
    //    ContainerManagerSecurityInfo.class, SecurityInfo.class);
    return (ContainerManager) rpc.getProxy(ContainerManager.class, adr, conf);
  }

  // usage $0 nmAddr user [fstokens]
  public static void main(String[] argv) throws Exception {
    Configuration conf = new Configuration();
    InetSocketAddress nmAddr = NetUtils.createSocketAddr(argv[0]);
    ContainerManager client = getClient(conf, nmAddr);
    Path tokens = (argv.length > 2) ? new Path(argv[2]) : null;
    ContainerLaunchContext ctxt = getContainer(conf, 0, 0, argv[1], tokens);
    client.startContainer(ctxt);
    System.out.println("START: " + ctxt);
  }

}
