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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.local.LocalFs;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ApplicationLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.FSDownload;

import org.apache.hadoop.yarn.api.records.URL;

import static org.apache.hadoop.yarn.api.records.LocalResourceType.*;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.*;

import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import static org.mockito.Mockito.*;

public class TestApplicationLocalizer {

  static final Path basedir =
      new Path("target", TestApplicationLocalizer.class.getName());

  private static final FsPermission urwx =
    FsPermission.createImmutable((short) 0700);
  private static final FsPermission urwx_gx =
    FsPermission.createImmutable((short) 0710);
  private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  static DataInputBuffer createFakeCredentials(Random r, int nTok)
      throws IOException {
    Credentials creds = new Credentials();
    byte[] password = new byte[20];
    Text kind = new Text();
    Text service = new Text();
    Text alias = new Text();
    for (int i = 0; i < nTok; ++i) {
      byte[] identifier = ("idef" + i).getBytes();
      r.nextBytes(password);
      kind.set("kind" + i);
      service.set("service" + i);
      alias.set("token" + i);
      Token token = new Token(identifier, password, kind, service);
      creds.addToken(alias, token);
    }
    DataOutputBuffer buf = new DataOutputBuffer();
    creds.writeTokenStorageToStream(buf);
    DataInputBuffer ret = new DataInputBuffer();
    ret.reset(buf.getData(), 0, buf.getLength());
    return ret;
  }

  static Collection<org.apache.hadoop.yarn.api.records.LocalResource> createFakeResources(Random r, int nFiles,
      Map<Long,org.apache.hadoop.yarn.api.records.LocalResource> sizes) throws IOException {
    ArrayList<org.apache.hadoop.yarn.api.records.LocalResource> rsrc = new ArrayList<org.apache.hadoop.yarn.api.records.LocalResource>();
    long basetime = r.nextLong() >>> 2;
    for (int i = 0; i < nFiles; ++i) {
      org.apache.hadoop.yarn.api.records.LocalResource resource = recordFactory.newRecordInstance(org.apache.hadoop.yarn.api.records.LocalResource.class);
      URL path = recordFactory.newRecordInstance(URL.class);
      path.setScheme("file");
//      path.host = null;
      path.setPort(0);
      resource.setTimestamp(basetime + i);
      r.setSeed(resource.getTimestamp());
      sizes.put(r.nextLong() & Long.MAX_VALUE, resource);
      StringBuilder sb = new StringBuilder("/" + r.nextLong());
      while (r.nextInt(2) == 1) {
        sb.append("/" + r.nextLong());
      }
      path.setFile(sb.toString());
      resource.setResource(path);
      resource.setSize(-1);
      resource.setType(r.nextInt(2) == 1 ? FILE : ARCHIVE);
      resource.setVisibility(PRIVATE);
      rsrc.add(resource);
    }
    return rsrc;
  }

  static DataInputBuffer writeFakeAppFiles(Collection<org.apache.hadoop.yarn.api.records.LocalResource> rsrc)
      throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    ApplicationLocalizer.writeResourceDescription(dob, rsrc);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    return dib;
  }

  @Test
  public void testLocalizationMain() throws IOException, InterruptedException {
    Configuration conf = new Configuration();
    AbstractFileSystem spylfs =
      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    // don't actually create dirs
    doNothing().when(spylfs).mkdir(Matchers.<Path>anyObject(),
        Matchers.<FsPermission>anyObject(), anyBoolean());
    FileContext lfs = FileContext.getFileContext(spylfs, conf);

    // TODO mocked FileContext requires relative paths; LTC will provide abs
    List<Path> localDirs = new ArrayList<Path>();
    for (int i = 0; i < 4; ++i) {
      localDirs.add(new Path(basedir,
            new Path(i + "", ApplicationLocalizer.USERCACHE)));
    }

    final Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    // return credential stream instead of opening local file
    DataInputBuffer appTokens = createFakeCredentials(r, 10);
    Path tokenPath =
      lfs.makeQualified(new Path(ApplicationLocalizer.APPTOKEN_FILE));
    doReturn(new FSDataInputStream(new FakeFSDataInputStream(appTokens))
        ).when(spylfs).open(tokenPath);
    // return file stream instead of opening local file
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    final HashMap<Long,org.apache.hadoop.yarn.api.records.LocalResource> sizes = new HashMap<Long,org.apache.hadoop.yarn.api.records.LocalResource>();
    Collection<org.apache.hadoop.yarn.api.records.LocalResource> resources = createFakeResources(r, 10, sizes);
    DataInputBuffer appFiles = writeFakeAppFiles(resources);
    Path filesPath =
      lfs.makeQualified(new Path(ApplicationLocalizer.FILECACHE_FILE));
    doReturn(new FSDataInputStream(new FakeFSDataInputStream(appFiles))
        ).when(spylfs).open(filesPath);

    final String user = "yak";
    final String appId = "app_RM_0";
    final InetSocketAddress nmAddr = new InetSocketAddress("foobar", 4344);
    final Path logDir = new Path(basedir, "logs");
    ApplicationLocalizer localizer = new ApplicationLocalizer(lfs, user,
        appId, logDir, localDirs);
    ApplicationLocalizer spyLocalizer = spy(localizer);
    LocalizationProtocol mockLocalization = mock(LocalizationProtocol.class);
    FSDownload mockDownload = mock(FSDownload.class);

    // set to return mocks
    doReturn(mockLocalization).when(spyLocalizer).getProxy(nmAddr);
    for (Map.Entry<Long,org.apache.hadoop.yarn.api.records.LocalResource> rsrc : sizes.entrySet()) {
      doReturn(new FalseDownload(rsrc.getValue(), rsrc.getKey())
          ).when(spyLocalizer).download(Matchers.<LocalDirAllocator>anyObject(),
            argThat(new LocalResourceMatches(rsrc.getValue())));
    }
    assertEquals(0, spyLocalizer.runLocalization(nmAddr));

    // verify app files opened
    verify(spylfs).open(tokenPath);
    verify(spylfs).open(filesPath);
    ArgumentMatcher<String> userMatch =
      new ArgumentMatcher<String>() {
        @Override
        public boolean matches(Object o) {
          return "yak".equals(o.toString());
        }
      };
    for (final Map.Entry<Long,org.apache.hadoop.yarn.api.records.LocalResource> rsrc : sizes.entrySet()) {
      ArgumentMatcher<org.apache.hadoop.yarn.api.records.LocalResource> localizedMatch =
        new ArgumentMatcher<org.apache.hadoop.yarn.api.records.LocalResource>() {
          @Override
          public boolean matches(Object o) {
            org.apache.hadoop.yarn.api.records.LocalResource other = (org.apache.hadoop.yarn.api.records.LocalResource) o;
            r.setSeed(rsrc.getValue().getTimestamp());
            boolean ret = (r.nextLong() & Long.MAX_VALUE) == other.getSize();
            StringBuilder sb = new StringBuilder("/" + r.nextLong());
            while (r.nextInt(2) == 1) {
              sb.append("/" + r.nextLong());
            }
            ret &= other.getResource().getFile().equals(sb.toString());
            ret &= other.getType().equals(r.nextInt(2) == 1 ? FILE : ARCHIVE);
            return ret;
          }
        };
      ArgumentMatcher<URL> dstMatch =
        new ArgumentMatcher<URL>() {
          @Override
          public boolean matches(Object o) {
            r.setSeed(rsrc.getValue().getTimestamp());
            return ((URL)o).getFile().equals(
                "/done/" + (r.nextLong() & Long.MAX_VALUE));
          }
        };
        
      ArgumentMatcher<SuccessfulLocalizationRequest> sucLocMatch = new ArgumentMatcher<SuccessfulLocalizationRequest>() {

        @Override
        public boolean matches(Object o) {
          SuccessfulLocalizationRequest req = (SuccessfulLocalizationRequest)o;
          
          //UserMatch
          String user = req.getUser();
          boolean retUser = "yak".equals(user.toString());
          
          //LocalResourceMatch
          org.apache.hadoop.yarn.api.records.LocalResource other = req.getResource();
          r.setSeed(rsrc.getValue().getTimestamp());
          boolean retLocalResource = (r.nextLong() & Long.MAX_VALUE) == other.getSize();
          StringBuilder sb = new StringBuilder("/" + r.nextLong());
          while (r.nextInt(2) == 1) {
            sb.append("/" + r.nextLong());
          }
          retLocalResource &= other.getResource().getFile().equals(sb.toString());
          retLocalResource &= other.getType().equals(r.nextInt(2) == 1 ? FILE : ARCHIVE);
          
          //Path Match
          URL url = req.getPath();
          r.setSeed(rsrc.getValue().getTimestamp());
          boolean retUrl = (url.getFile().equals(
              "/done/" + (r.nextLong() & Long.MAX_VALUE)));
          
          return (retUser && retLocalResource && retUrl); 
        }

      };
        
        verify(mockLocalization).successfulLocalization(argThat(sucLocMatch));
    }
  }

  static class FalseDownload extends FSDownload {
    private final long size;
    public FalseDownload(org.apache.hadoop.yarn.api.records.LocalResource resource, long size) {
      super(null, null, null, resource, null);
      this.size = size;
    }
    @Override
    public Map<org.apache.hadoop.yarn.api.records.LocalResource,Path> call() {
      org.apache.hadoop.yarn.api.records.LocalResource ret = getResource();
      ret.setSize(size);
      return Collections.singletonMap(ret, new Path("/done/" + size));
    }
  }

  // sigh.
  class LocalResourceMatches extends ArgumentMatcher<org.apache.hadoop.yarn.api.records.LocalResource> {
    final org.apache.hadoop.yarn.api.records.LocalResource rsrc;
    LocalResourceMatches(org.apache.hadoop.yarn.api.records.LocalResource rsrc) {
      this.rsrc = rsrc;
    }
    @Override
    public boolean matches(Object o) {
      return rsrc.getTimestamp() == ((org.apache.hadoop.yarn.api.records.LocalResource)o).getTimestamp();
    }
  }

}
