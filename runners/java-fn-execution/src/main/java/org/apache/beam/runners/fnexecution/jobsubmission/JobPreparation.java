package org.apache.beam.runners.fnexecution.jobsubmission;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Struct;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;

/** A job that has been prepared, but not invoked. */
@AutoValue
public abstract class JobPreparation {
  public static Builder builder() {
    return new AutoValue_JobPreparation.Builder();
  }

  public abstract String id();
  public abstract Pipeline pipeline();
  public abstract Struct options();
  public abstract GrpcFnServer<ArtifactStagingService> stagingService();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setId(String id);
    abstract Builder setPipeline(Pipeline pipeline);
    abstract Builder setOptions(Struct options);
    abstract Builder setStagingService(GrpcFnServer<ArtifactStagingService> stagingService);
    abstract JobPreparation build();
  }
}
