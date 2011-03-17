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
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;

public class MasterKeyGenerator {

  public static void main(String[] args) throws IOException,
      InterruptedException {
    final Configuration myConf = new Configuration();
    UserGroupInformation.setConfiguration(myConf);
    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation clientUgi = UserGroupInformation.getLoginUser();
      clientUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          generateMasterKey(myConf);
          return null;
        }
      });
    }
  }

  // For now RM and NM share the secret key through a file. This should be
  // changed to RPC based sharing. TODO:
  public static void generateMasterKey(Configuration conf) throws IOException {
    ApplicationTokenSecretManager appSecretManager =
        new ApplicationTokenSecretManager();
    Credentials masterSecretKey = new Credentials();
    masterSecretKey.addSecretKey(YarnConfiguration.RMNMMasterKeyAliasName,
        appSecretManager.getMasterKey().getEncoded());
    Path masterKeyFile =
        new Path(YarnConfiguration.MASTER_KEYS_DIR,
            YarnConfiguration.MASTER_KEY_FILE_NAME);
    masterSecretKey.writeTokenStorageFile(masterKeyFile, conf);
  }
}
