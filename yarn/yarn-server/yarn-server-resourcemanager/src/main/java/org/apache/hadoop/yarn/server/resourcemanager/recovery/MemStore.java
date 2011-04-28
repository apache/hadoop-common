package org.apache.hadoop.yarn.server.resourcemanager.recovery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeManager;

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

public class MemStore implements Store {
  RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private NodeId nodeId;

  public MemStore() {
    nodeId = recordFactory.newRecordInstance(NodeId.class);
    nodeId.setId(-1);
  }

  @Override
  public void storeNode(NodeManager node) throws IOException {}

  @Override
  public void removeNode(NodeManager node) throws IOException {}

  @Override
  public void storeContainer(Container container) throws IOException {}

  @Override
  public void removeContainer(Container container) throws IOException {}

  @Override
  public void storeApplication(ApplicationId application,
      ApplicationSubmissionContext context, ApplicationMaster master) throws IOException {}

  @Override
  public void removeApplication(ApplicationId application) throws IOException {}

  @Override
  public void updateApplicationState(ApplicationId applicationId,
      ApplicationMaster master) throws IOException {}

  @Override
  public RMState restore() throws IOException {
    MemRMState state = new MemRMState();
    return state;
  }

  @Override
  public synchronized NodeId getNextNodeId() throws IOException {
    int num = nodeId.getId();
    num++;
    nodeId.setId(num);
    return nodeId;
  }

  private class MemRMState implements RMState {

    public MemRMState() {
      nodeId = recordFactory.newRecordInstance(NodeId.class);
    }

    @Override
    public List<NodeManager> getStoredNodeManagers() throws IOException {
      return new ArrayList<NodeManager>();
    }

    @Override
    public List<ApplicationSubmissionContext> getStoredSubmissionContexts()
    throws IOException {
      return new ArrayList<ApplicationSubmissionContext>();
    }

    @Override
    public NodeId getLastLoggedNodeId() throws IOException {
      return nodeId;
    }

    @Override
    public List<ApplicationMaster> getStoredAMs() throws IOException {
      return new ArrayList<ApplicationMaster>();
    }
  }
}