package org.apache.beam.runners.flink.execution;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.sdk.util.ThrowingSupplier;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link JobResourceManager}.
 */
public class JobResourceManagerTest {

  private ProvisionApi.ProvisionInfo jobInfo = ProvisionApi.ProvisionInfo.newBuilder().build();
  private RunnerApi.Environment environment = RunnerApi.Environment.newBuilder().build();
  private @Mock ArtifactSource artifactSource;
  private @Mock JobResourceFactory jobResourceFactory;
  private @Mock EnvironmentManager containerManager;
  private @Mock RemoteEnvironment remoteEnvironment;
  private @Mock GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private @Mock GrpcFnServer<GrpcDataService> dataServer;
  private @Mock GrpcFnServer<GrpcStateService> stateServer;
  private @Mock GrpcDataService dataService;
  private @Mock InstructionRequestHandler requestHandler;

  private ThrowingSupplier<InstructionRequestHandler> requestHandlerSupplier = () -> requestHandler;

  private JobResourceManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    manager = JobResourceManager.create(jobInfo, environment, artifactSource, jobResourceFactory);
    when(jobResourceFactory.dataService()).thenReturn(dataServer);
    when(jobResourceFactory.stateService()).thenReturn(stateServer);
    when(dataServer.getService()).thenReturn(dataService);
    when(jobResourceFactory.containerManager(artifactSource, jobInfo, controlServer,
        requestHandlerSupplier))
        .thenReturn(containerManager);
    when(containerManager.getEnvironment(environment)).thenReturn(remoteEnvironment);
  }

  @Test
  public void testStartCreatesResources() throws Exception {
    manager.start();
    verify(jobResourceFactory, times(1))
        .containerManager(artifactSource, jobInfo, controlServer, requestHandlerSupplier);
    verify(containerManager, times(1)).getEnvironment(environment);
    assertTrue(manager.isStarted());
  }

  @Test
  public void testGetEnvironmentSucceedsIfStarted() throws Exception {
    manager.start();
    EnvironmentSession session = manager.getSession();
    assertNotNull(session);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetEnvironmentFailsIfNotStarted() throws Exception {
    manager.getSession();
  }

}
