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

package org.apache.hadoop.yarn.util;

import java.io.IOException;

import javax.crypto.SecretKey;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.KerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.ApplicationID;

public class SecurityUtil {

  private static final Log LOG = LogFactory.getLog(SecurityUtil.class);

  // imitation of operations done by client to upload FileSystem
  // DelegationTokens
  public static void uploadFileSystemDelegationTokens(Configuration conf,
      ApplicationID appId, Path[] resourcePaths, Credentials credentials)
      throws IOException {

    getFileSystemTokens(conf, resourcePaths, credentials);

    FileSystem defaultFS = FileSystem.get(conf);
    // TODO: fix
    credentials.writeTokenStorageFile(
        new Path("yarn", Integer.toString(appId.id),
            YarnConfiguration.FS_TOKENS_FILE_NAME).makeQualified(
            FileSystem.getDefaultUri(conf), defaultFS.getWorkingDirectory()),
        conf);
  }

  public static void getFileSystemTokens(Configuration conf,
      Path[] resourcePaths, Credentials credentials) throws IOException {
    // get JobManager principal id (for the renewer)
    KerberosName jmKrbName =
        new KerberosName(
            conf.get(YarnConfiguration.APPLICATION_MANAGER_PRINCIPAL, ""));
    String delegTokenRenewer = jmKrbName.getShortName();

    for (Path path : resourcePaths) {
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      Token<?> token = fs.getDelegationToken(delegTokenRenewer);
      String fsName = fs.getCanonicalServiceName();
      if (token != null) {
        Text fsNameText = new Text(fsName);
        token.setService(fsNameText);
        credentials.addToken(fsNameText, token);
        LOG.info("Got delegationToken for " + fs.getUri() + ";uri=" + fsName
            + ";t.service=" + token.getService());
      }
    }
  }

  // TODO: ApplicationMaster needs one token for each NodeManager. This should
  // be created by ResourceManager and sent to ApplicationMaster via RPC.
  public static void loadContainerManagerTokens(ApplicationID appId,
      Configuration conf, String nmServiceAddress) throws IOException {

    Path masterKeyFile =
        new Path(YarnConfiguration.MASTER_KEYS_DIR,
            YarnConfiguration.MASTER_KEY_FILE_NAME);
    Credentials allMasterSecretKeys =
        Credentials.readTokenStorageFile(masterKeyFile, conf);
    byte[] masterKeyBytes =
        allMasterSecretKeys
            .getSecretKey(YarnConfiguration.RMNMMasterKeyAliasName);
    SecretKey masterKey =
        ApplicationTokenSecretManager.createSecretKey(masterKeyBytes);

    ApplicationTokenSecretManager appSecretManager =
        new ApplicationTokenSecretManager();
    appSecretManager.setMasterKey(masterKey);

    // create JobToken file and write token to it
    ApplicationTokenIdentifier identifier =
        new ApplicationTokenIdentifier(appId);
    Token<ApplicationTokenIdentifier> token =
        new Token<ApplicationTokenIdentifier>(identifier, appSecretManager);
    token.setService(new Text(nmServiceAddress));

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.addToken(token);

    for (Token<? extends TokenIdentifier> t : ugi.getTokens()) {
      LOG.info("Token added for service " + t.getService()
          + " with identifer " + t.getIdentifier());
    }
  }
}
