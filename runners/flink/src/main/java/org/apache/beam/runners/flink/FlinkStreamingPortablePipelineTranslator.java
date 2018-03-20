/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Translate an unbounded portable pipeline representation into a Flink pipeline representation.
 */
public class FlinkStreamingPortablePipelineTranslator implements FlinkPortablePipelineTranslator<
        FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext> {

  interface StreamingTranslationContext extends FlinkPortablePipelineTranslator.TranslationContext {
    StreamExecutionEnvironment getExecutionEnvironment();
    <T> void addDataStream(String pCollectionId, DataStream<T> dataStream);
    <T> DataStream<T> getDataStreamOrThrow(String pCollectionId);
  }

  private class StreamingTranslationContextImpl
          extends TranslationContextImpl
          implements StreamingTranslationContext {
    private final StreamExecutionEnvironment executionEnvironment;
    private final Map<String, DataStream<?>> dataStreams;
    private StreamingTranslationContextImpl(
            PipelineOptions pipelineOptions,
            StreamExecutionEnvironment executionEnvironment) {
      super(pipelineOptions);
      this.executionEnvironment = executionEnvironment;
      dataStreams = new HashMap<>();
    }

    @Override
    public StreamExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }

    @Override
    public <T> void addDataStream(String pCollectionId, DataStream<T> dataSet) {
      dataStreams.put(pCollectionId, dataSet);
    }

    @Override
    public <T> DataStream<T> getDataStreamOrThrow(String pCollectionId) {
      DataStream<T> dataSet = (DataStream<T>) dataStreams.get(pCollectionId);
      if (dataSet == null) {
        throw new IllegalArgumentException(
                String.format("Unknown dataset for id %s.", pCollectionId));
      }
      return dataSet;
    }

  }

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  private final Map<String, PTransformTranslator<StreamingTranslationContext>>
          urnToTransformTranslator = new HashMap<>();

  FlinkStreamingPortablePipelineTranslator() {
    urnToTransformTranslator.put(PTransformTranslation.FLATTEN_TRANSFORM_URN,
            this::translateFlatten);
    urnToTransformTranslator.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
            this::translateGroupByKey);
    urnToTransformTranslator.put(PTransformTranslation.IMPULSE_TRANSFORM_URN,
            this::translateImpulse);
    urnToTransformTranslator.put(ExecutableStage.URN, this::translateExecutableStage);
  }


  @Override
  public void translate(StreamingTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator.getOrDefault(
              transform.getTransform().getSpec().getUrn(), this::urnNotFound)
              .translate(transform.getId(), pipeline, context);
    }

  }

  public void urnNotFound(
          String id, RunnerApi.Pipeline pipeline, FlinkBatchPortablePipelineTranslator.TranslationContext context) {
    throw new IllegalArgumentException(
            String.format("Unknown type of URN %s for PTrasnform with id %s.",
                    pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(),
                    id));
  }

  public void translateFlatten(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    throw new UnsupportedOperationException();
  }

  public void translateGroupByKey(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    throw new UnsupportedOperationException();
  }

  public void translateImpulse(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    throw new UnsupportedOperationException();
  }

  public void translateExecutableStage(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    throw new UnsupportedOperationException();
  }

}
