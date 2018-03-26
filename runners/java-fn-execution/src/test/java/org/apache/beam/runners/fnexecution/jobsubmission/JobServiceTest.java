package org.apache.beam.runners.fnexecution.jobsubmission;

import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingServiceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link JobService}. */
@RunWith(JUnit4.class)
public class JobServiceTest {
  @Mock
  GrpcFnServer<ArtifactStagingService> artifactStagingServer;
  @Mock
  JobInvoker invoker;

  JobService service;
  GrpcFnServer<JobService> server;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    ArtifactStagingServiceProvider provider = ignored -> artifactStagingServer;
    service = JobService.create(provider, invoker);
    server = GrpcFnServer.allocatePortAndCreateFor(service, InProcessServerFactory.create());
  }

  @After
  public void tearDown() throws Exception {
    server.close();
  }

  @Test
  public void testJobSuccessfullyProcessed() throws Exception {
    // TODO: prepare and start a job

  }
}
