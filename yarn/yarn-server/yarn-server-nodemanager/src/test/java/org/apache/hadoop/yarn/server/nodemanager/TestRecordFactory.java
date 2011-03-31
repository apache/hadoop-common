package org.apache.hadoop.yarn.server.nodemanager;

import junit.framework.Assert;

import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.impl.pb.FailedLocalizationRequestPBImpl;
import org.junit.Test;

public class TestRecordFactory {
  
  @Test
  public void testPbRecordFactory() {
    RecordFactory pbRecordFactory = RecordFactoryPBImpl.get();
    
    try {
      FailedLocalizationRequest response = pbRecordFactory.newRecordInstance(FailedLocalizationRequest.class);
      Assert.assertEquals(FailedLocalizationRequestPBImpl.class, response.getClass());
    } catch (YarnException e) {
      e.printStackTrace();
      Assert.fail("Failed to crete record");
    }
  }

}
