package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.api.RMAdminProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.SchedulerSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.service.AbstractService;

public class AdminService extends AbstractService implements RMAdminProtocol {

  private static final Log LOG = LogFactory.getLog(AdminService.class);

  private final Configuration conf;
  private final ResourceScheduler scheduler;

  private Server server;
  private InetSocketAddress masterServiceAddress;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public AdminService(Configuration conf, ResourceScheduler scheduler) {
    super(AdminService.class.getName());
    this.conf = conf;
    this.scheduler = scheduler;
  }

  @Override
  public void init(Configuration conf) {
    String bindAddress =
      conf.get(RMConfig.ADMIN_ADDRESS,
          RMConfig.DEFAULT_ADMIN_BIND_ADDRESS);
    masterServiceAddress =  NetUtils.createSocketAddr(bindAddress);
    super.init(conf);
  }

  public void start() {
    YarnRPC rpc = YarnRPC.create(getConfig());
    Configuration serverConf = new Configuration(getConfig());
    serverConf.setClass(
        CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
        SchedulerSecurityInfo.class, SecurityInfo.class);
    this.server =
      rpc.getServer(RMAdminProtocol.class, this, masterServiceAddress,
          serverConf, null,
          serverConf.getInt(RMConfig.RM_ADMIN_THREADS, 
              RMConfig.DEFAULT_RM_ADMIN_THREADS));
    this.server.start();
    super.start();
  }

  @Override
  public void stop() {
    if (this.server != null) {
      this.server.close();
    }
    super.stop();
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws YarnRemoteException {
    try {
      scheduler.reinitialize(conf, null, null); // ContainerTokenSecretManager can't
                                          // be 'refreshed'
      RefreshQueuesResponse response = 
        recordFactory.newRecordInstance(RefreshQueuesResponse.class);
      return response;
    } catch (IOException ioe) {
      LOG.info("Exception refreshing queues ", ioe);
      throw RPCUtil.getRemoteException(ioe);
    }
  }
}