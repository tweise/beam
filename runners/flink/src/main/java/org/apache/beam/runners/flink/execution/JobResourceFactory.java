package org.apache.beam.runners.flink.execution;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.artifact.GrpcArtifactProxyService;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClientControlService;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.DockerWrapper;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.SingletonDockerEnvironmentManager;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.LogWriter;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;

/**
 * Factory for resources that are managed by {@link JobResourceManager}.
 */
public class JobResourceFactory {

  public static JobResourceFactory create(ServerFactory serverFactory, ExecutorService executor) {
    return new JobResourceFactory(serverFactory, executor);
  }

  private final ServerFactory serverFactory;
  private final ExecutorService executor;

  private JobResourceFactory(ServerFactory serverFactory, ExecutorService executor) {
    this.serverFactory = serverFactory;
    this.executor = executor;
  }

  /** Create a new logging service. */
  private GrpcFnServer<GrpcLoggingService> loggingService() throws IOException {
    LogWriter logWriter = Slf4jLogWriter.getDefault();
    GrpcLoggingService loggingService = GrpcLoggingService.forWriter(logWriter);
    return GrpcFnServer.allocatePortAndCreateFor(loggingService, serverFactory);
  }

  /** Create a new artifact retrieval service. */
  private GrpcFnServer<ArtifactRetrievalService> artifactRetrievalService(
      ArtifactSource artifactSource) throws IOException {
    ArtifactRetrievalService retrievalService = GrpcArtifactProxyService.fromSource(artifactSource);
    return GrpcFnServer.allocatePortAndCreateFor(retrievalService, serverFactory);
  }

  /** Create a new provisioning service. */
  private GrpcFnServer<StaticGrpcProvisionService> provisionService(ProvisionInfo jobInfo)
      throws IOException {
    StaticGrpcProvisionService provisioningService = StaticGrpcProvisionService.create(jobInfo);
    return GrpcFnServer.allocatePortAndCreateFor(provisioningService, serverFactory);
  }

  /** Create a new control service. */
  private GrpcFnServer<SdkHarnessClientControlService> controlService(GrpcDataService dataService)
      throws IOException {
    SdkHarnessClientControlService controlService =
        SdkHarnessClientControlService.create(() -> dataService);
    return GrpcFnServer.allocatePortAndCreateFor(controlService, serverFactory);
  }

  /** Create a new data service. */
  public GrpcFnServer<GrpcDataService> dataService() throws IOException {
    GrpcDataService dataService = GrpcDataService.create(executor);
    return GrpcFnServer.allocatePortAndCreateFor(dataService, serverFactory);
  }

  public GrpcFnServer<GrpcStateService> stateService() throws IOException {
    GrpcStateService stateService  = new GrpcStateService();
    return GrpcFnServer.allocatePortAndCreateFor(stateService, serverFactory);
  }

  /** Create a new container manager from artifact source and jobInfo. */
  EnvironmentManager containerManager(
      ArtifactSource artifactSource, ProvisionInfo jobInfo, GrpcDataService dataService)
      throws IOException {
    return SingletonDockerEnvironmentManager.forServices(
        // TODO: Replace hardcoded values with configurable ones
        DockerWrapper.forCommand("docker", Duration.ofSeconds(30)),
        controlService(dataService),
        loggingService(),
        artifactRetrievalService(artifactSource),
        provisionService(jobInfo)
    );
  }

}
