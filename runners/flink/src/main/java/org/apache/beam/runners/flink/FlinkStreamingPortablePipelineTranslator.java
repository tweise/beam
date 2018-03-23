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

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
    urnToTransformTranslator.put(ExecutableStage.URN,
        this::translateExecutableStage);
    urnToTransformTranslator.put(PTransformTranslation.RESHUFFLE_URN,
        this::translateReshuffle);
  }


  @Override
  public void translate(StreamingTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p = QueryablePipeline.forTransforms(
        pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator.getOrDefault(
              transform.getTransform().getSpec().getUrn(), this::urnNotFound)
              .translate(transform.getId(), pipeline, context);
    }

  }

  public void urnNotFound(
          String id, RunnerApi.Pipeline pipeline,
          FlinkBatchPortablePipelineTranslator.TranslationContext context) {
    throw new IllegalArgumentException(
            String.format("Unknown type of URN %s for PTrasnform with id %s.",
                    pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(),
                    id));
  }

  private <K, V> void translateReshuffle(
      String id,
      RunnerApi.Pipeline pipeline,
      StreamingTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(id);
    DataStream<WindowedValue<KV<K, V>>> inputDataSet =
        context.getDataStreamOrThrow(
            Iterables.getOnlyElement(transform.getInputsMap().values()));
    context.addDataStream(Iterables.getOnlyElement(transform.getOutputsMap().values()),
        inputDataSet.rebalance());
  }

  public <T>  void translateFlatten(
          String id,
          RunnerApi.Pipeline pipeline,
          StreamingTranslationContext context) {
    Map<String, String> allInputs =
            pipeline.getComponents().getTransformsOrThrow(id).getInputsMap();

    if (allInputs.isEmpty()) {

      // create an empty dummy source to satisfy downstream operations
      // we cannot create an empty source in Flink, therefore we have to
      // add the flatMap that simply never forwards the single element
      DataStreamSource<String> dummySource =
              context.getExecutionEnvironment().fromElements("dummy");

      DataStream<WindowedValue<T>> result =
              dummySource
                      .<WindowedValue<T>>flatMap(
                              (s, collector) -> {
                                // never return anything
                              })
                      .returns(
                              new CoderTypeInformation<>(
                                      WindowedValue.getFullCoder(
                                              (Coder<T>) VoidCoder.of(),
                                              GlobalWindow.Coder.INSTANCE)));
      context.addDataStream(Iterables.getOnlyElement(
              pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()), result);
    } else {
      DataStream<T> result = null;

      // Determine DataStreams that we use as input several times. For those, we need to uniquify
      // input streams because Flink seems to swallow watermarks when we have a union of one and
      // the same stream.
      Map<DataStream<T>, Integer> duplicates = new HashMap<>();
      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);
        Integer oldValue = duplicates.put(current, 1);
        if (oldValue != null) {
          duplicates.put(current, oldValue + 1);
        }
      }

      for (String input : allInputs.values()) {
        DataStream<T> current = context.getDataStreamOrThrow(input);

        final Integer timesRequired = duplicates.get(current);
        if (timesRequired > 1) {
          current = current.flatMap(new FlatMapFunction<T, T>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(T t, Collector<T> collector) throws Exception {
              collector.collect(t);
            }
          });
        }
        result = (result == null) ? current : result.union(current);
      }

      context.addDataStream(Iterables.getOnlyElement(
              pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()), result);
    }
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
