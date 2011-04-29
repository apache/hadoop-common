package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class LocalizerContext {

  private final String user;
  private final ContainerId container;
  private final Credentials credentials;

  public LocalizerContext(String user, ContainerId container,
      Credentials credentials) {
    this.user = user;
    this.container = container;
    this.credentials = credentials;
  }

  public String getUser() {
    return user;
  }

  public ContainerId getContainer() {
    return container;
  }

  public Credentials getCredentials() {
    return credentials;
  }

}
