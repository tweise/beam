package org.apache.beam.runners.flink;

import org.apache.beam.model.pipeline.v1.RunnerApi;

public interface FlinkPortablePipelineTranslator {
  void translate(FlinkPipelineExecutionEnvironment environment, RunnerApi.Pipeline pipeline);
}
