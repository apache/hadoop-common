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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.avro.ipc.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YARNApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.AvroUtil;
import org.apache.hadoop.yarn.ApplicationID;
import org.apache.hadoop.yarn.ApplicationMaster;
import org.apache.hadoop.yarn.ApplicationState;
import org.apache.hadoop.yarn.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.LocalResource;
import org.apache.hadoop.yarn.LocalResourceType;
import org.apache.hadoop.yarn.LocalResourceVisibility;
import org.apache.hadoop.yarn.Resource;
import org.apache.hadoop.yarn.URL;

/**
 * This class enables the current JobClient (0.22 hadoop) to run on YARN.
 */
public class YARNRunner implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  public static final String YARN_AM_RESOURCE_KEY = "yarn.am.mapreduce.resource.mb";
  private static final int DEFAULT_YARN_AM_RESOURCE = 1024;
  
  private ResourceMgrDelegate resMgrDelegate;
  private ClientServiceDelegate clientServiceDelegate;
  private YarnConfiguration conf;

  /**
   * Yarn runner incapsulates the client interface of
   * yarn
   * @param conf the configuration object for the client
   */
  public YARNRunner(Configuration conf)
      throws AvroRemoteException {
    this.conf = new YarnConfiguration(conf);
    try {
      this.resMgrDelegate = new ResourceMgrDelegate(conf);
      this.clientServiceDelegate = new ClientServiceDelegate(conf,
          resMgrDelegate);
    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    resMgrDelegate.cancelDelegationToken(arg0);
  }

  @Override
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
	  throw new IOException("Not implemented");
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
   return resMgrDelegate.getAllJobs();
  }

  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    return resMgrDelegate.getBlacklistedTrackers();
  }

  @Override
  public QueueInfo[] getChildQueues(String arg0) throws IOException,
      InterruptedException {
    return resMgrDelegate.getChildQueues(arg0);
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {  
    return resMgrDelegate.getClusterMetrics();
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text arg0)
      throws IOException, InterruptedException {
    return resMgrDelegate.getDelegationToken(arg0);
  }

  @Override
  public String getFilesystemName() throws IOException, InterruptedException {
    return resMgrDelegate.getFilesystemName();
  }

  @Override
  public JobID getNewJobID() throws IOException, InterruptedException {
    return resMgrDelegate.getNewJobID();
  }

  @Override
  public QueueInfo getQueue(String arg0) throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueue(arg0);
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    return resMgrDelegate.getQueueAclsForCurrentUser();
  }

  @Override
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getQueues();
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    return resMgrDelegate.getRootQueues();
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    return resMgrDelegate.getStagingAreaDir();
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    return resMgrDelegate.getSystemDir();
  }
  
  @Override
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    return resMgrDelegate.getTaskTrackerExpiryInterval();
  }
  
  @Override
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
  throws IOException, InterruptedException{

    // Upload only in security mode: TODO
    Path applicationTokensFile =
        new Path(jobSubmitDir, YarnConfiguration.APPLICATION_TOKENS_FILE);
    try {
      ts.writeTokenStorageFile(applicationTokensFile, conf);
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // XXX Remove
    Path submitJobDir = new Path(jobSubmitDir);
    FileContext defaultFS = FileContext.getFileContext(conf);
    Path submitJobFile =
      defaultFS.makeQualified(JobSubmissionFiles.getJobConfPath(submitJobDir));
    FSDataInputStream in = defaultFS.open(submitJobFile);
    conf.addResource(in);
    // ---

    // Construct necessary information to start the MR AM
    ApplicationSubmissionContext appContext = 
      getApplicationSubmissionContext(conf, jobSubmitDir, ts);
    setupDistributedCache(conf, appContext);
    
    // XXX Remove
    in.close();
    // ---
    
    // Submit to ResourceManager
    ApplicationID applicationId = resMgrDelegate.submitApplication(appContext);
    
    ApplicationMaster appMaster = 
      resMgrDelegate.getApplicationMaster(applicationId);
    if (appMaster.state == ApplicationState.FAILED || appMaster.state ==
      ApplicationState.KILLED) {
      throw new AvroRemoteException("failed to run job");
    }
    return clientServiceDelegate.getJobStatus(jobId);
  }

  private LocalResource createApplicationResource(FileContext fs, Path p)
      throws IOException {
    LocalResource rsrc = new LocalResource();
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.resource = AvroUtil.getYarnUrlFromPath(rsrcStat.getPath());
    rsrc.size = rsrcStat.getLen();
    rsrc.timestamp = rsrcStat.getModificationTime();
    rsrc.type = LocalResourceType.FILE;
    rsrc.state = LocalResourceVisibility.APPLICATION;
    return rsrc;
  }

  private ApplicationSubmissionContext getApplicationSubmissionContext(
      Configuration jobConf,
      String jobSubmitDir, Credentials ts) throws IOException {
    ApplicationSubmissionContext appContext =
        new ApplicationSubmissionContext();
    ApplicationID applicationId = resMgrDelegate.getApplicationId();
    appContext.applicationId = applicationId;
    Resource capability = new Resource();
    capability.memory =
        conf.getInt(YARN_AM_RESOURCE_KEY, DEFAULT_YARN_AM_RESOURCE);
    LOG.info("Master capability = " + capability);
    appContext.masterCapability = capability;

    FileContext defaultFS = FileContext.getFileContext(conf);
    Path jobConfPath = new Path(jobSubmitDir, YARNApplicationConstants.JOB_CONF_FILE);
    
    URL yarnUrlForJobSubmitDir =
        AvroUtil.getYarnUrlFromPath(defaultFS.makeQualified(new Path(
            jobSubmitDir)));
    appContext.resources = new HashMap<CharSequence, URL>();
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    appContext.resources.put(YARNApplicationConstants.JOB_SUBMIT_DIR,
        yarnUrlForJobSubmitDir);

    appContext.resources_todo = new HashMap<CharSequence,LocalResource>();
    appContext.resources_todo.put(YARNApplicationConstants.JOB_CONF_FILE,
          createApplicationResource(defaultFS,
          jobConfPath));
    appContext.resources_todo.put(YARNApplicationConstants.JOB_JAR,
          createApplicationResource(defaultFS,
            new Path(jobSubmitDir, YARNApplicationConstants.JOB_JAR)));
    // TODO gross hack
    for (String s : new String[] { "job.split", "job.splitmetainfo",
        YarnConfiguration.APPLICATION_TOKENS_FILE }) {
      appContext.resources_todo.put(
          YARNApplicationConstants.JOB_SUBMIT_DIR + "/" + s,
          createApplicationResource(defaultFS,
            new Path(jobSubmitDir, s)));
    }

    // TODO: Only if security is on.
    List<CharSequence> fsTokens = new ArrayList<CharSequence>();
    for (Token<? extends TokenIdentifier> token : ts.getAllTokens()) {
      fsTokens.add(token.encodeToUrlString());
    }
    
    // TODO - Remove this!
    appContext.fsTokens = fsTokens;
    DataOutputBuffer dob = new DataOutputBuffer();
    ts.writeTokenStorageToStream(dob);
    appContext.fsTokens_todo =
      ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Add queue information
    appContext.queue = 
      jobConf.get(JobContext.QUEUE_NAME, JobConf.DEFAULT_QUEUE_NAME);
    
    // Add job name
    appContext.applicationName = jobConf.get(JobContext.JOB_NAME, "N/A");
    
    // Add the command line
    String javaHome = "$JAVA_HOME";
    Vector<CharSequence> vargs = new Vector<CharSequence>(8);
    vargs.add(javaHome + "/bin/java");
    vargs.add(conf.get(YARNApplicationConstants.MR_APPMASTER_COMMAND_OPTS,
        "-Dhadoop.root.logger=DEBUG,console -Xmx1024m"));

    // Add { job jar, MR app jar } to classpath.
    appContext.environment = new HashMap<CharSequence, CharSequence>();
    MRApps.setInitialClasspath(appContext.environment);
    MRApps.addToClassPath(appContext.environment,
        YARNApplicationConstants.JOB_JAR);
    MRApps.addToClassPath(appContext.environment,
        YARNApplicationConstants.YARN_MAPREDUCE_APP_JAR_PATH);
    vargs.add("org.apache.hadoop.mapreduce.v2.app.MRAppMaster");
    vargs.add(String.valueOf(applicationId.clusterTimeStamp));
    vargs.add(String.valueOf(applicationId.id));
    vargs.add("1>logs/stderr");
    vargs.add("2>logs/stdout");

    Vector<CharSequence> vargsFinal = new Vector<CharSequence>(8);
    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add("mkdir logs;" + mergedCommand.toString());

    LOG.info("Command to launch container for ApplicationMaster is : "
        + mergedCommand);

    appContext.command = vargsFinal;
    // TODO: RM should get this from RPC.
    appContext.user = UserGroupInformation.getCurrentUser().getShortUserName();
    return appContext;
  }
  
  /**
   * TODO: Copied for now from TaskAttemptImpl.java ... fixme
   */
  private void setupDistributedCache(Configuration conf, 
      ApplicationSubmissionContext container) throws IOException {
    
    // Cache archives
    parseDistributedCacheArtifacts(container, LocalResourceType.ARCHIVE, 
        DistributedCache.getCacheArchives(conf), 
        DistributedCache.getArchiveTimestamps(conf), 
        getFileSizes(conf, MRJobConfig.CACHE_ARCHIVES_SIZES), 
        DistributedCache.getArchiveVisibilities(conf), 
        DistributedCache.getArchiveClassPaths(conf));
    
    // Cache files
    parseDistributedCacheArtifacts(container, LocalResourceType.FILE, 
        DistributedCache.getCacheFiles(conf),
        DistributedCache.getFileTimestamps(conf),
        getFileSizes(conf, MRJobConfig.CACHE_FILES_SIZES),
        DistributedCache.getFileVisibilities(conf),
        DistributedCache.getFileClassPaths(conf));
  }

  // TODO - Move this to MR!
  // Use TaskDistributedCacheManager.CacheFiles.makeCacheFiles(URI[], long[], boolean[], Path[], FileType)
  private static void parseDistributedCacheArtifacts(
      ApplicationSubmissionContext container, LocalResourceType type,
      URI[] uris, long[] timestamps, long[] sizes, boolean visibilities[], 
      Path[] classpaths) throws IOException {

    if (uris != null) {
      // Sanity check
      if ((uris.length != timestamps.length) || (uris.length != sizes.length) ||
          (uris.length != visibilities.length)) {
        throw new IllegalArgumentException("Invalid specification for " +
            "distributed-cache artifacts of type " + type + " :" +
            " #uris=" + uris.length +
            " #timestamps=" + timestamps.length +
            " #visibilities=" + visibilities.length
            );
      }
      
      Map<String, Path> classPaths = new HashMap<String, Path>();
      if (classpaths != null) {
        for (Path p : classpaths) {
          classPaths.put(p.toUri().getPath().toString(), p);
        }
      }
      for (int i = 0; i < uris.length; ++i) {
        URI u = uris[i];
        Path p = new Path(u.toString());
        // Add URI fragment or just the filename
        Path name = new Path((null == u.getFragment())
          ? p.getName()
          : u.getFragment());
        if (name.isAbsolute()) {
          throw new IllegalArgumentException("Resource name must be relative");
        }
        container.resources_todo.put(
            name.toUri().getPath(),
            getLocalResource(
                uris[i], type, 
                visibilities[i]
                  ? LocalResourceVisibility.PUBLIC
                  : LocalResourceVisibility.PRIVATE,
                sizes[i], timestamps[i])
        );
        if (classPaths.containsKey(u.getPath())) {
          MRApps.addToClassPath(container.environment, name.toUri().getPath());
        }
      }
    }
  }

  // TODO - Move this to MR!
  private static long[] getFileSizes(Configuration conf, String key) {
    String[] strs = conf.getStrings(key);
    if (strs == null) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }
  
  private static LocalResource getLocalResource(URI uri, 
      LocalResourceType type, LocalResourceVisibility visibility, 
      long size, long timestamp) {
    LocalResource resource = new LocalResource();
    resource.resource = AvroUtil.getYarnUrlFromURI(uri);
    resource.type = type;
    resource.state = visibility;
    resource.size = size;
    resource.timestamp = timestamp;
    return resource;
  }
  
  @Override
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    resMgrDelegate.setJobPriority(arg0, arg1);
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return resMgrDelegate.getProtocolVersion(arg0, arg1);
  }
  
  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    return resMgrDelegate.renewDelegationToken(arg0);
  }

  
  @Override
  public Counters getJobCounters(JobID arg0) throws IOException,
      InterruptedException {
    return clientServiceDelegate.getJobCounters(arg0);
  }

  @Override
  public String getJobHistoryDir() throws IOException, InterruptedException {
    return clientServiceDelegate.getJobHistoryDir();
  }

  @Override
  public JobStatus getJobStatus(JobID jobID) throws IOException,
      InterruptedException {
    JobStatus status = clientServiceDelegate.getJobStatus(jobID);
    return status;
  }
  
  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    return clientServiceDelegate.getTaskCompletionEvents(arg0, arg1, arg2);
  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException,
      InterruptedException {
    return clientServiceDelegate.getTaskDiagnostics(arg0);
  }

  @Override
  public TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
  throws IOException, InterruptedException {
    return clientServiceDelegate
        .getTaskReports(jobID, taskType);
  }

  @Override
  public void killJob(JobID arg0) throws IOException, InterruptedException {
    clientServiceDelegate.killJob(arg0);
  }

  @Override
  public boolean killTask(TaskAttemptID arg0, boolean arg1) throws IOException,
      InterruptedException {
    return clientServiceDelegate.killTask(arg0, arg1);
  }

  @Override
  public AccessControlList getQueueAdmins(String arg0) throws IOException {
    return new AccessControlList("*");
  }
}
