package org.apache.beam.runners.fnexecution.jobsubmission;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Factory to create a {@link JobInvocation} instances.
 */
public interface JobInvoker {
  /**
   * Create a {@link JobInvocation} instance from a {@link JobPreparation} and an artifact token.
   */
  JobInvocation invoke(JobPreparation preparation, @Nullable String artifactToken)
      throws IOException;
}
