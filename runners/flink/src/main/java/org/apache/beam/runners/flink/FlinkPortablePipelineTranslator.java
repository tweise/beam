package org.apache.beam.runners.flink;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;

/** Interface for portable pipeline translators. */
public interface FlinkPortablePipelineTranslator<T> {

  /**
   * TranslationContext, shared between batch and streaming.
   */
  interface TranslationContext {
    PipelineOptions getPipelineOptions();
  }

  /**
   * TranslationContextImpl, shared between batch and streaming.
   */
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
