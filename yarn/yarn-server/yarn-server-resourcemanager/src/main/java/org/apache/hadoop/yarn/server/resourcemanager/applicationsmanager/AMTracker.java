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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.ApplicationStatus;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.Container;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.ASMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * This class tracks the application masters that are running. It tracks
 * heartbeats from application master to see if it needs to expire some application
 * master.
 */
@Evolving
@Private
public class AMTracker extends AbstractService  implements EventHandler<ASMEvent
    <ApplicationEventType>> {
  private static final Log LOG = LogFactory.getLog(AMTracker.class);
  private HeartBeatThread heartBeatThread;
  private long amExpiryInterval; 
  @SuppressWarnings("rawtypes")
  private EventHandler handler;

  private final ASMContext asmContext;
  
  private final Map<ApplicationID, ApplicationMasterInfo> applications = 
    new ConcurrentHashMap<ApplicationID, ApplicationMasterInfo>();

  private TreeSet<ApplicationStatus> amExpiryQueue =
    new TreeSet<ApplicationStatus>(
        new Comparator<ApplicationStatus>() {
          public int compare(ApplicationStatus p1, ApplicationStatus p2) {
            if (p1.lastSeen < p2.lastSeen) {
              return -1;
            } else if (p1.lastSeen > p2.lastSeen) {
              return 1;
            } else {
              return (p1.applicationId.id -
                  p2.applicationId.id);
            }
          }
        }
    );
  
  public AMTracker(ASMContext asmContext) {
    super(AMTracker.class.getName());
    this.heartBeatThread = new HeartBeatThread();
    this.asmContext = asmContext;
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    this.handler = asmContext.getDispatcher().getEventHandler();
    this.amExpiryInterval = conf.getLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 
    YarnConfiguration.DEFAULT_AM_EXPIRY_INTERVAL);
    this.asmContext.getDispatcher().register(ApplicationEventType.class, this);
  }

  @Override
  public void start() {   
    super.start();
    heartBeatThread.start();
  }

  /**
   * This class runs continuosly to track the application masters
   * that might be dead.
   */
  private class HeartBeatThread extends Thread {
    private volatile boolean stop = false;

    public HeartBeatThread() {
      super("ApplicationsManager:" + HeartBeatThread.class.getName());
    }

    @Override
    public void run() {
      /* the expiry queue does not need to be in sync with applications,
       * if an applications in the expiry queue cannot be found in applications
       * its alright. We do not want to hold a hold on applications while going
       * through the expiry queue.
       */
      List<ApplicationID> expired = new ArrayList<ApplicationID>();
      while (!stop) {
        ApplicationStatus leastRecent;
        long now = System.currentTimeMillis();
        expired.clear();
        synchronized(amExpiryQueue) {
          while ((amExpiryQueue.size() > 0) &&
              (leastRecent = amExpiryQueue.first()) != null &&
              ((now - leastRecent.lastSeen) > 
              amExpiryInterval)) {
            amExpiryQueue.remove(leastRecent);
            ApplicationMasterInfo info;
            synchronized(applications) {
              info = applications.get(leastRecent.applicationId);
            }
            if (info == null) {
              continue;
            }
            ApplicationStatus status = info.getStatus();
            if ((now - status.lastSeen) > amExpiryInterval) {
              expired.add(status.applicationId);
            } else {
              amExpiryQueue.add(status);
            }
          }
        }
        expireAMs(expired);
      }
    }

    public void shutdown() {
      stop = true;
    }
  }

  protected void expireAMs(List<ApplicationID> toExpire) {
    for (ApplicationID app: toExpire) {
      ApplicationMasterInfo am = null;
      synchronized (applications) {
        am = applications.get(app);
      }
     
      handler.handle(new ASMEvent<ApplicationEventType>
          (ApplicationEventType.EXPIRE, am));
      }
    }

  @Override
  public void stop() {
    heartBeatThread.interrupt();
    heartBeatThread.shutdown();
    try {
      heartBeatThread.join(1000);
    } catch (InterruptedException ie) {
      LOG.info(heartBeatThread.getName() + " interrupted during join ", 
          ie);    }
    super.stop();
  }

  public void addMaster(String user,  ApplicationSubmissionContext 
      submissionContext, String clientToken) {
    ApplicationMasterInfo applicationMaster = new ApplicationMasterInfo(handler, 
      user, submissionContext, clientToken);
    synchronized(applications) {
      applications.put(applicationMaster.getApplicationID(), applicationMaster);
    }
    /* initiate the launching cycle for the AM */
    handler.handle(new ASMEvent<ApplicationEventType>(
        ApplicationEventType.ALLOCATE, applicationMaster));
  }
  
  public void finish(ApplicationID application) {
    ApplicationMasterInfo masterInfo = null;
    synchronized(applications) {
      masterInfo = applications.get(application);
    }
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.FINISH,
        masterInfo));
  }

  public ApplicationMasterInfo get(ApplicationID applicationId) {
    ApplicationMasterInfo masterInfo = null;
    synchronized (applications) {
      masterInfo = applications.get(applicationId);
    }
    return masterInfo;
  }

  public void remove(ApplicationID applicationId) {
    synchronized (applications) {
      applications.remove(applicationId);
    }
  }

  public synchronized List<AppContext> getAllApplications() {
    List<AppContext> allAMs = new ArrayList<AppContext>();
    synchronized (applications) {
      for ( ApplicationMasterInfo val: applications.values()) {
        allAMs.add(val);
      }
    }
    return allAMs;
  } 

  private void addForTracking(AppContext master) {
    LOG.info("Adding application master for tracking " + master.getMaster());
    synchronized (amExpiryQueue) {
      amExpiryQueue.add(master.getStatus());
    }
  }

  public void kill(ApplicationID applicationID) {
    ApplicationMasterInfo masterInfo = null;
    
    synchronized(applications) {
      masterInfo = applications.get(applicationID);
    }
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.KILL, 
    masterInfo));
  }
  
  /*
   * this class is used for passing status context to the application state
   * machine.
   */
  private static class TrackerAppContext implements AppContext {
    private final ApplicationID appID;
    private final ApplicationMaster master;
    private final UnsupportedOperationException notimplemented;
  
    public TrackerAppContext(
         ApplicationID appId, ApplicationMaster master) {
      this.appID = appId;
      this.master = master;
      this.notimplemented = new NotImplementedException();
    }
    
    @Override
    public ApplicationSubmissionContext getSubmissionContext() {
      throw notimplemented;
    }
    @Override
    public Resource getResource() {
      throw notimplemented;
    }
    @Override
    public ApplicationID getApplicationID() {
      return appID;
    }
    @Override
    public ApplicationStatus getStatus() {
      return master.status;
    }
    @Override
    public ApplicationMaster getMaster() {
      return master;
    }
    @Override
    public Container getMasterContainer() {
      throw notimplemented;
    }
    @Override
    public String getUser() {   
      throw notimplemented;
    }
    @Override
    public long getLastSeen() {
      return master.status.lastSeen;
    }
    @Override
    public String getName() {
     throw notimplemented;
    }
    @Override
    public String getQueue() {
      throw notimplemented;
    }
  }
  
  public void heartBeat(ApplicationStatus status) {
    ApplicationMaster master = new ApplicationMaster();
    master.status = status;
    master.applicationId = status.applicationId;
    TrackerAppContext context = new TrackerAppContext(status.applicationId, master);
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.STATUSUPDATE, 
        context));
  }
  
  public void registerMaster(ApplicationMaster applicationMaster) {
    applicationMaster.status.lastSeen = System.currentTimeMillis();
    ApplicationMasterInfo master = null;
    synchronized(applications) {
      master = applications.get(applicationMaster.applicationId);
    }
    LOG.info("AM registration " + master.getMaster());
    TrackerAppContext registrationContext = new TrackerAppContext(
        master.getApplicationID(), applicationMaster);
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.
      REGISTERED,  registrationContext));
  }
  
  @Override
  public void handle(ASMEvent<ApplicationEventType> event) {
    ApplicationID appID = event.getAppContext().getApplicationID();
    ApplicationMasterInfo masterInfo = null;
    synchronized(applications) {
      masterInfo = applications.get(appID);
    }
    try {
      masterInfo.handle(event);
    } catch(Throwable t) {
      LOG.error("Error in handling event type " + event.getType() + " for application " 
          + event.getAppContext().getApplicationID());
    }
    /* we need to launch the applicaiton master on allocated transition */
    if (masterInfo.getState() == ApplicationState.ALLOCATED) {
      handler.handle(new ASMEvent<ApplicationEventType>(
        ApplicationEventType.LAUNCH, masterInfo));
    }
    if (masterInfo.getState() == ApplicationState.LAUNCHED) {
      /* the application move to a launched state start tracking */
      synchronized (amExpiryQueue) {
        LOG.info("DEBUG -- adding to  expiry " + masterInfo.getStatus() + 
        " currenttime " + System.currentTimeMillis());
        amExpiryQueue.add(masterInfo.getStatus());
      }
    }
  }
}