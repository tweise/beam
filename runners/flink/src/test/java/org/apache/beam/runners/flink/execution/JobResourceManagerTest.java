package org.apache.beam.runners.flink.execution;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.environment.EnvironmentManager;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link JobResourceManager}.
 */
public class JobResourceManagerTest {

  ProvisionApi.ProvisionInfo jobInfo = ProvisionApi.ProvisionInfo.newBuilder().build();
  RunnerApi.Environment environment = RunnerApi.Environment.newBuilder().build();
  @Mock ArtifactSource artifactSource;
  @Mock JobResourceFactory jobResourceFactory;
  @Mock EnvironmentManager containerManager;
  @Mock RemoteEnvironment remoteEnvironment;

  JobResourceManager manager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    manager = JobResourceManager.create(jobInfo, environment, artifactSource, jobResourceFactory);
    when(jobResourceFactory.containerManager(artifactSource, jobInfo)).thenReturn(containerManager);
    when(containerManager.getEnvironment(environment)).thenReturn(remoteEnvironment);
  }

  @Test
  public void testStartCreatesResources() throws Exception {
    manager.start();
    verify(jobResourceFactory, times(1)).containerManager(artifactSource, jobInfo);
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
