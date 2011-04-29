package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;

import org.junit.Test;
import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_LOCAL_DIR;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.DEFAULT_NM_LOCAL_DIR;

public class TestResourceLocalizationService {

  static final Path basedir =
      new Path("target", TestResourceLocalizationService.class.getName());

  @Test
  public void testLocalizationInit() throws Exception {
    final Configuration conf = new Configuration();
    AsyncDispatcher dispatcher = new AsyncDispatcher();
    dispatcher.init(null);

    ContainerExecutor exec = mock(ContainerExecutor.class);
    DeletionService delService = spy(new DeletionService(exec));
    delService.init(null);
    delService.start();

    AbstractFileSystem spylfs =
      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
    FileContext lfs = FileContext.getFileContext(spylfs, conf);
    doNothing().when(spylfs).mkdir(
        isA(Path.class), isA(FsPermission.class), anyBoolean());

    ResourceLocalizationService locService =
      spy(new ResourceLocalizationService(dispatcher, exec, delService));
    doReturn(lfs)
      .when(locService).getLocalFileContext(isA(Configuration.class));
    try {
      dispatcher.start();
      List<Path> localDirs = new ArrayList<Path>();
      String[] sDirs = new String[4];
      for (int i = 0; i < 4; ++i) {
        localDirs.add(lfs.makeQualified(new Path(basedir, i + "")));
        sDirs[i] = localDirs.get(i).toString();
      }
      conf.setStrings(NM_LOCAL_DIR, sDirs);

      // initialize ResourceLocalizationService
      locService.init(conf);

      // verify directory creation
      for (Path p : localDirs) {
        Path usercache = new Path(p, ContainerLocalizer.USERCACHE);
        verify(spylfs)
          .mkdir(eq(usercache), isA(FsPermission.class), eq(true));
        Path publicCache = new Path(p, ContainerLocalizer.FILECACHE);
        verify(spylfs)
          .mkdir(eq(publicCache), isA(FsPermission.class), eq(true));
        Path nmPriv = new Path(p, ResourceLocalizationService.NM_PRIVATE_DIR);
        verify(spylfs).mkdir(eq(nmPriv),
            eq(ResourceLocalizationService.NM_PRIVATE_PERM), eq(true));
      }
    } finally {
      dispatcher.stop();
      delService.stop();
    }
  }

}
