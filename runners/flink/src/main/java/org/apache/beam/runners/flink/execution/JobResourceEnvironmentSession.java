package org.apache.beam.runners.flink.execution;

import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;

/**
 * An environment session that is managed by a JobResourceManager.
 */
public class JobResourceEnvironmentSession implements EnvironmentSession {
  private final RunnerApi.Environment environment;
  private final ArtifactSource artifactSource;
  private final SdkHarnessClient client;
  private final ApiServiceDescriptor dataServiceDescriptor;

  private boolean isClosed = false;

  public JobResourceEnvironmentSession(
      RunnerApi.Environment environment,
      ArtifactSource artifactSource,
      SdkHarnessClient client,
      ApiServiceDescriptor dataServiceDescriptor) {
    this.environment = environment;
    this.artifactSource = artifactSource;
    this.client = client;
    this.dataServiceDescriptor = dataServiceDescriptor;
  }

  @Override
  public RunnerApi.Environment getEnvironment() {
    return environment;
  }

  @Override
  public ArtifactSource getArtifactSource() {
    validateNotClosed();
    return artifactSource;
  }

  @Override
  public SdkHarnessClient getClient() {
    validateNotClosed();
    return client;
  }

  @Override
  public ApiServiceDescriptor getDataServiceDescriptor() {
    return dataServiceDescriptor;
  }

  /**
   * Throw a runtime exception if the session has been closed.
   */
  private void validateNotClosed() {
    if (this.isClosed) {
      throw new IllegalStateException("EnvironmentSession was used after close() was called.");
    }
  }

  @Override
  public void close() throws Exception {
    // TODO: eventually use this for reference counting open sessions
    this.isClosed = true;
  }
}
