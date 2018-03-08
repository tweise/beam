package org.apache.beam.runners.flink;

import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.runners.fnexecution.jobsubmission.JobPreparation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job Invoker for the {@link FlinkRunner}.
 */
public class FlinkJobInvoker implements JobInvoker {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvoker.class);

  public static FlinkJobInvoker create(ListeningExecutorService executorService) {
    return new FlinkJobInvoker(executorService);
  }

  private final ListeningExecutorService executorService;

  private FlinkJobInvoker(ListeningExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public JobInvocation invoke(JobPreparation preparation, @Nullable String artifactToken)
      throws IOException {
    String invocationId =
        String.format("%s_%d", preparation.id(), ThreadLocalRandom.current().nextInt());
    LOG.debug("Creating new JobInvocation: %s", invocationId);
    // TODO: handle empty struct intelligently
    // PipelineOptions options = PipelineOptionsTranslation.fromProto(preparation.options());
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(FlinkRunner.class);

    Pipeline pipeline = PipelineTranslation.fromProto(preparation.pipeline());
    FlinkRunner runner = FlinkRunner.fromOptions(options);
    ArtifactSource artifactSource = preparation.stagingService().getService().createAccessor();
    runner.setArtifactSource(artifactSource);
    return FlinkJobInvocation.create(invocationId, executorService, runner, pipeline);
  }
}
