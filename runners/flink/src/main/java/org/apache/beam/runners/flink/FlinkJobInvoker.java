package org.apache.beam.runners.flink;

import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
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
    LOG.debug("Invoking job preparation {}", preparation.id());
    String invocationId =
        String.format("%s_%d", preparation.id(), ThreadLocalRandom.current().nextInt());
    // TODO(axelmagn): handle empty struct intelligently
    LOG.trace("Parsing pipeline options");
    /*
    FlinkPipelineOptions options =
        (FlinkPipelineOptions) PipelineOptionsTranslation.fromProto(preparation.options());
        */
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setUsePortableRunner(true);

    LOG.trace("Translating pipeline from proto");
    // TODO(axelmagn): remove this hack once python pipeline is working
    RunnerApi.Pipeline origPipeline = preparation.pipeline();
    RunnerApi.Environment hackEnv =
        RunnerApi.Environment
            .newBuilder()
            .setUrl("gcr.io/google.com/hadoop-cloud-dev/beam/python")
            .build();
    RunnerApi.Components hackComponents =
        RunnerApi.Components
            .newBuilder(origPipeline.getComponents())
            .putEnvironments("", hackEnv)
            .build();
    RunnerApi.Pipeline hackPipeline =
        RunnerApi.Pipeline
            .newBuilder(origPipeline)
            .setComponents(hackComponents)
            .build();
    Pipeline pipeline = PipelineTranslation.fromProto(hackPipeline);

    LOG.trace("Creating flink runner");
    FlinkRunner runner = FlinkRunner.fromOptions(options);
    ArtifactSource artifactSource = preparation.stagingService().getService().createAccessor();
    runner.setArtifactSource(artifactSource);

    return FlinkJobInvocation.create(invocationId, executorService, runner, pipeline);
  }
}
