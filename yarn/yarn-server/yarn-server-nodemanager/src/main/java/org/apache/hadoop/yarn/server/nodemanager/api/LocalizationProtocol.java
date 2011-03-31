package org.apache.hadoop.yarn.server.nodemanager.api;

import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.FailedLocalizationResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationRequest;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.SuccessfulLocalizationResponse;

public interface LocalizationProtocol {
  public SuccessfulLocalizationResponse successfulLocalization(SuccessfulLocalizationRequest request) throws YarnRemoteException;
  public FailedLocalizationResponse failedLocalization(FailedLocalizationRequest request) throws YarnRemoteException;
}
