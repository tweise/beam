package org.apache.beam.runners.flink;

import org.apache.beam.model.pipeline.v1.RunnerApi;

/** Interface for portable pipeline translators. */
public interface FlinkPortablePipelineTranslator<T> {
  void translate(T context, RunnerApi.Pipeline pipeline);
}
