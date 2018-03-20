package org.apache.beam.runners.flink;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;

/** Interface for portable pipeline translators. */
public interface FlinkPortablePipelineTranslator<T> {

  // Shared between batch and streaming
  interface TranslationContext {
    PipelineOptions getPipelineOptions();
  }

  abstract class TranslationContextImpl
          implements TranslationContext {
    private final PipelineOptions pipelineOptions;

    protected TranslationContextImpl(PipelineOptions pipelineOptions) {
      this.pipelineOptions = pipelineOptions;
    }

    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }
  }

  void translate(T context, RunnerApi.Pipeline pipeline);
}
