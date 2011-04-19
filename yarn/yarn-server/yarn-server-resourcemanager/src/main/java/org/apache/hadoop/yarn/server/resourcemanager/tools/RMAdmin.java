package org.apache.hadoop.yarn.server.resourcemanager.tools;

import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.admin.AdminSecurityInfo;
import org.apache.hadoop.yarn.server.resourcemanager.api.RMAdminProtocol;
import org.apache.hadoop.yarn.server.resourcemanager.api.protocolrecords.RefreshQueuesRequest;

public class RMAdmin extends Configured implements Tool {

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  public RMAdmin() {
    super();
  }

  public RMAdmin(Configuration conf) {
    super(conf);
  }

  private static void printHelp(String cmd) {
    String summary = "rmadmin is the command to execute Map-Reduce administrative commands.\n" +
    "The full syntax is: \n\n" +
    "hadoop rmadmin [-refreshQueues] " +
    "[-help [cmd]]\n";

    String refreshQueues =
      "-refreshQueues: Reload the queues' acls, states and "
      + "scheduler specific properties.\n"
      + "\t\tJobTracker will reload the mapred-queues configuration file.\n";

    String help = "-help [cmd]: \tDisplays help for the given command or all commands if none\n" +
    "\t\tis specified.\n";

    if ("refreshQueues".equals(cmd)) {
      System.out.println(refreshQueues);
    } else if ("help".equals(cmd)) {
      System.out.println(help);
    } else {
      System.out.println(summary);
      System.out.println(refreshQueues);
      System.out.println(help);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.out);
    }
  }

  /**
   * Displays format of commands.
   * @param cmd The command that is being executed.
   */
  private static void printUsage(String cmd) {
    if ("-refreshQueues".equals(cmd)) {
      System.err.println("Usage: java RMAdmin" + " [-refreshQueues]");
    } else {
      System.err.println("Usage: java RMAdmin");
      System.err.println("           [-refreshQueues]");
      System.err.println("           [-help [cmd]]");
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
    }
  }

  private static UserGroupInformation getUGI(Configuration conf
  ) throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  private int refreshQueues() throws IOException {
    // Get the current configuration
    final YarnConfiguration conf = new YarnConfiguration(getConf());

    // Create the client
    final String adminAddress =
      conf.get(YarnConfiguration.ADMIN_ADDRESS,
          YarnConfiguration.DEFAULT_ADMIN_BIND_ADDRESS);
    final YarnRPC rpc = YarnRPC.create(conf);
    
    if (UserGroupInformation.isSecurityEnabled()) {
      conf.setClass(CommonConfigurationKeys.HADOOP_SECURITY_INFO_CLASS_NAME,
          AdminSecurityInfo.class, SecurityInfo.class);
    }
    
    RMAdminProtocol adminProtocol =
      getUGI(conf).doAs(new PrivilegedAction<RMAdminProtocol>() {
        @Override
        public RMAdminProtocol run() {
          return (RMAdminProtocol) rpc.getProxy(RMAdminProtocol.class,
              NetUtils.createSocketAddr(adminAddress), conf);
        }
      });

    // Refresh the queue properties
    RefreshQueuesRequest request = 
      recordFactory.newRecordInstance(RefreshQueuesRequest.class);
    adminProtocol.refreshQueues(request);

    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    int i = 0;
    String cmd = args[i++];
    //
    // verify that we have enough command line parameters
    //
    if ("-refreshQueues".equals(cmd)) {
      if (args.length != 1) {
        printUsage(cmd);
        return exitCode;
      }
    }

    exitCode = 0;
    try {
      if ("-refreshQueues".equals(cmd)) {
        exitCode = refreshQueues();
      } else if ("-help".equals(cmd)) {
        if (i < args.length) {
          printUsage(args[i]);
        } else {
          printHelp("");
        }
      } else {
        exitCode = -1;
        System.err.println(cmd.substring(1) + ": Unknown command");
        printUsage("");
        printUsage("");
      }

    } catch (IllegalArgumentException arge) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage());
      printUsage(cmd);
    } catch (RemoteException e) {
      //
      // This is a error returned by hadoop server. Print
      // out the first line of the error mesage, ignore the stack trace.
      exitCode = -1;
      try {
        String[] content;
        content = e.getLocalizedMessage().split("\n");
        System.err.println(cmd.substring(1) + ": "
                           + content[0]);
      } catch (Exception ex) {
        System.err.println(cmd.substring(1) + ": "
                           + ex.getLocalizedMessage());
      }
    } catch (Exception e) {
      exitCode = -1;
      System.err.println(cmd.substring(1) + ": "
                         + e.getLocalizedMessage());
    }
    return exitCode;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new RMAdmin(), args);
    System.exit(result);
  }
}
