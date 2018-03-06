package org.apache.beam.runners.fnexecution.artifact;

import java.io.IOException;
import org.apache.beam.runners.fnexecution.GrpcFnServer;

/**
 * An interface that will provide artifact staging services for individual jobs.
 */
public interface ArtifactStagingServiceProvider {
  GrpcFnServer<ArtifactStagingService> forJob(String jobPreparationId) throws IOException;
}
