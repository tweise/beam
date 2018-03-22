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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkExecutableStagePruningFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkPartialReduceFunction;
import org.apache.beam.runners.flink.translation.functions.FlinkReduceFunction;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.KvKeySelector;
import org.apache.beam.runners.flink.translation.wrappers.ImpulseInputFormat;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.MapPartitionOperator;

/**
 * Translate a bounded portable pipeline representation into a Flink pipeline representation.
 */
public class FlinkBatchPortablePipelineTranslator
    implements FlinkPortablePipelineTranslator<
    FlinkBatchPortablePipelineTranslator.BatchTranslationContext> {

  public static BatchTranslationContext createBatchContext(FlinkPipelineOptions options) {
    return new BatchTranslationContextImpl(
        options,
        FlinkPipelineExecutionEnvironment.createBatchExecutionEnvironment(options));
  }

  // For batch only
  interface BatchTranslationContext extends FlinkPortablePipelineTranslator.TranslationContext {
    ExecutionEnvironment getExecutionEnvironment();
    <T> void addDataSet(String pCollectionId, DataSet<T> dataSet);
    <T> DataSet<T> getDataSetOrThrow(String pCollectionId);
    Collection<DataSet<?>> getDanglingDataSets();
  }

  private static class BatchTranslationContextImpl
      extends TranslationContextImpl
      implements BatchTranslationContext {

    private final ExecutionEnvironment executionEnvironment;
    private final Map<String, DataSet<?>> dataSets;
    private final Set<String> danglingDataSets;

    private BatchTranslationContextImpl(
        PipelineOptions pipelineOptions,
        ExecutionEnvironment executionEnvironment) {
      super(pipelineOptions);
      this.executionEnvironment = executionEnvironment;
      dataSets = new HashMap<>();
      danglingDataSets = new HashSet<>();
    }

    @Override
    public ExecutionEnvironment getExecutionEnvironment() {
      return executionEnvironment;
    }

    @Override
    public <T> void addDataSet(String pCollectionId, DataSet<T> dataSet) {
      checkArgument(!dataSets.containsKey(pCollectionId));
      dataSets.put(pCollectionId, dataSet);
      danglingDataSets.add(pCollectionId);
    }

    @Override
    public <T> DataSet<T> getDataSetOrThrow(String pCollectionId) {
      DataSet<T> dataSet = (DataSet<T>) dataSets.get(pCollectionId);
      if (dataSet == null) {
        throw new IllegalArgumentException(
                String.format("Unknown dataset for id %s.", pCollectionId));
      }
      // Assume that the DataSet is consumed if requested.
      danglingDataSets.remove(pCollectionId);
      return dataSet;
    }

    @Override
    public Collection<DataSet<?>> getDanglingDataSets() {
      return danglingDataSets.stream().map(id -> dataSets.get(id)).collect(Collectors.toList());
    }

  }

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  private final Map<String, PTransformTranslator<BatchTranslationContext>>
      urnToTransformTranslator = new HashMap<>();

  FlinkBatchPortablePipelineTranslator() {
    urnToTransformTranslator.put(PTransformTranslation.FLATTEN_TRANSFORM_URN,
        this::translateFlatten);
    urnToTransformTranslator.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
        this::translateGroupByKey);
    urnToTransformTranslator.put(PTransformTranslation.IMPULSE_TRANSFORM_URN,
        this::translateImpulse);
    urnToTransformTranslator.put(ExecutableStage.URN, this::translateExecutableStage);
  }

  @Override
  public void translate(
      BatchTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p = QueryablePipeline.forTransforms(
        pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator.getOrDefault(
          transform.getTransform().getSpec().getUrn(), this::urnNotFound)
          .translate(transform.getId(), pipeline, context);
    }

    for (DataSet<?> dataSet : context.getDanglingDataSets()) {
      dataSet.output(new DiscardingOutputFormat<>());
    }

  }

  private <InputT> void translateExecutableStage(
      String id,
      RunnerApi.Pipeline pipeline,
      BatchTranslationContext context) {
    // TODO: Fail on stateful DoFns for now.
    // TODO: Support stateful DoFns by inserting group-by-keys where necessary.
    // TODO: Fail on splittable DoFns.
    // TODO: Special-case single outputs to avoid multiplexing PCollections.

    RunnerApi.Components components = pipeline.getComponents();
    RunnerApi.PTransform transform = components.getTransformsOrThrow(id);
    Map<String, String> outputs = transform.getOutputsMap();
    // Mapping from local outputs to coder tag id.
    BiMap<String, Integer> outputMap = createOutputMap(outputs.keySet());
    // Collect all output Coders and create a UnionCoder for our tagged outputs.
    List<Coder<?>> unionCoders = Lists.newArrayList();
    // Enforce tuple tag sorting by union tag index.
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(components);
    Map<String, Coder<WindowedValue<?>>> outputCoders = Maps.newHashMap();
    for (String localOutputName : new TreeMap<>(outputMap.inverse()).values()) {
      String collectionId = outputs.get(localOutputName);
      RunnerApi.PCollection coll = components.getPcollectionsOrThrow(collectionId);
      RunnerApi.Coder coderProto = components.getCodersOrThrow(coll.getCoderId());
      Coder<WindowedValue<?>> windowCoder;
      try {
        Coder elementCoder = CoderTranslation.fromProto(coderProto, rehydratedComponents);
        RunnerApi.WindowingStrategy windowProto = components.getWindowingStrategiesOrThrow(
            coll.getWindowingStrategyId());
        WindowingStrategy<Object, BoundedWindow> windowingStrategy =
            (WindowingStrategy<Object, BoundedWindow>)
            WindowingStrategyTranslation.fromProto(windowProto, rehydratedComponents);
        windowCoder = WindowedValue.getFullCoder(elementCoder,
            windowingStrategy.getWindowFn().windowCoder());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      outputCoders.put(localOutputName, windowCoder);
      unionCoders.add(windowCoder);
    }
    UnionCoder unionCoder = UnionCoder.of(unionCoders);
    TypeInformation<RawUnionValue> typeInformation =
        new CoderTypeInformation<>(unionCoder);

    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload = RunnerApi.ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    FlinkExecutableStageFunction<InputT> function =
        new FlinkExecutableStageFunction<>(stagePayload, components,
            stagePayload.getEnvironment(),
            outputMap);
    Map<String, String> inputs = transform.getInputsMap();
    DataSet<WindowedValue<InputT>> inputDataSet =
        context.getDataSetOrThrow(Iterables.getOnlyElement(inputs.values()));
    DataSet<RawUnionValue> taggedDataset =
        new MapPartitionOperator<>(inputDataSet,
            typeInformation,
            function,
            transform.getUniqueName());

    for (Map.Entry<String, String> output : outputs.entrySet()) {
      pruneOutput(taggedDataset,
          context,
          outputMap.get(output.getKey()),
          (Coder) outputCoders.get(output.getKey()),
          transform.getUniqueName(),
          output.getValue());
    }
    if (outputs.isEmpty()) {
      // TODO: Is this still necessary?
      taggedDataset.output(new DiscardingOutputFormat());
    }
  }

  private <T> void translateFlatten(
      String id,
      RunnerApi.Pipeline pipeline,
      BatchTranslationContext context) {
    Map<String, String> allInputs =
        pipeline.getComponents().getTransformsOrThrow(id).getInputsMap();
    DataSet<WindowedValue<T>> result = null;

    if (allInputs.isEmpty()) {

      // create an empty dummy source to satisfy downstream operations
      // we cannot create an empty source in Flink, therefore we have to
      // add the flatMap that simply never forwards the single element
      DataSource<String> dummySource =
          context.getExecutionEnvironment().fromElements("dummy");
      result =
          dummySource
              .<WindowedValue<T>>flatMap(
                  (s, collector) -> {
                    // never return anything
                  })
              .returns(
                  new CoderTypeInformation<>(
                      WindowedValue.getFullCoder(
                          (Coder<T>) VoidCoder.of(), GlobalWindow.Coder.INSTANCE)));
    } else {
      for (String pCollectionId : allInputs.values()) {
        DataSet<WindowedValue<T>> current = context.getDataSetOrThrow(pCollectionId);
        if (result == null) {
          result = current;
        } else {
          result = result.union(current);
        }
      }
    }

    // insert a dummy filter, there seems to be a bug in Flink
    // that produces duplicate elements after the union in some cases
    // if we don't
    result = result.filter(tWindowedValue -> true).name("UnionFixFilter");
    context.addDataSet(Iterables.getOnlyElement(
        pipeline.getComponents().getTransformsOrThrow(id).getOutputsMap().values()),
        result);
  }

  private <K, V> void translateGroupByKey(
      String id, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    RunnerApi.PTransform pTransform =
        pipeline.getComponents().getTransformsOrThrow(id);
    String inputPCollectionId =
        Iterables.getOnlyElement(pTransform.getInputsMap().values());
    DataSet<WindowedValue<KV<K, V>>> inputDataSet =
        context.getDataSetOrThrow(inputPCollectionId);
    RunnerApi.Coder inputCoderProto =
        pipeline.getComponents().getCodersOrThrow(
            pipeline.getComponents().getPcollectionsOrThrow(
                inputPCollectionId).getCoderId());
    RunnerApi.WindowingStrategy windowingStrategyProto =
        pipeline.getComponents().getWindowingStrategiesOrThrow(
            pipeline.getComponents().getPcollectionsOrThrow(
                inputPCollectionId).getWindowingStrategyId());

    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipeline.getComponents());
    KvCoder<K, V> inputCoder;
    try {
      inputCoder = (KvCoder<K, V>) CoderTranslation.fromProto(
          inputCoderProto,
          rehydratedComponents);
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Unable to hydrate GroupByKey input coder %s.",
              inputCoderProto),
          e);
    }

    WindowingStrategy<Object, BoundedWindow> windowingStrategy;
    try {
      windowingStrategy =
          (WindowingStrategy<Object, BoundedWindow>) WindowingStrategyTranslation.fromProto(
              windowingStrategyProto,
              rehydratedComponents);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format("Unable to hydrate GroupByKey windowing strategy %s.",
              windowingStrategyProto),
          e);
    }

    Concatenate combineFn = new Concatenate<>();
    Coder<List<V>> accumulatorCoder = combineFn.getAccumulatorCoder(
          CoderRegistry.createDefault(),
          inputCoder.getValueCoder());

    TypeInformation<WindowedValue<KV<K, List<V>>>> partialReduceTypeInfo =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(
                KvCoder.of(inputCoder.getKeyCoder(), accumulatorCoder),
                windowingStrategy.getWindowFn().windowCoder()));

    Grouping<WindowedValue<KV<K, V>>> inputGrouping =
        inputDataSet.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));

    FlinkPartialReduceFunction<K, V, List<V>, ?> partialReduceFunction =
        new FlinkPartialReduceFunction<>(
            combineFn, windowingStrategy, Collections.emptyMap(), context.getPipelineOptions());

    FlinkReduceFunction<K, List<V>, List<V>, ?> reduceFunction =
        new FlinkReduceFunction<>(
            combineFn, windowingStrategy, Collections.emptyMap(), context.getPipelineOptions());

    // Partially GroupReduce the values into the intermediate format AccumT (combine)
    GroupCombineOperator<
        WindowedValue<KV<K, V>>,
        WindowedValue<KV<K, List<V>>>> groupCombine =
        new GroupCombineOperator<>(
            inputGrouping,
            partialReduceTypeInfo,
            partialReduceFunction,
            "GroupCombine: " + pTransform.getUniqueName());

    Grouping<WindowedValue<KV<K, List<V>>>> intermediateGrouping =
        groupCombine.groupBy(new KvKeySelector<>(inputCoder.getKeyCoder()));

    // Fully reduce the values and create output format VO
    GroupReduceOperator<
        WindowedValue<KV<K, List<V>>>, WindowedValue<KV<K, List<V>>>> outputDataSet =
        new GroupReduceOperator<>(
            intermediateGrouping,
            partialReduceTypeInfo,
            reduceFunction,
            pTransform.getUniqueName());

    context.addDataSet(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        outputDataSet);
  }

  private void translateImpulse(
      String id, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    RunnerApi.PTransform pTransform =
        pipeline.getComponents().getTransformsOrThrow(id);

    TypeInformation<WindowedValue<byte[]>> typeInformation =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));
    DataSource<WindowedValue<byte[]>> dataSource = new DataSource<>(
        context.getExecutionEnvironment(),
        new ImpulseInputFormat(),
        typeInformation,
        pTransform.getUniqueName());

    context.addDataSet(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        dataSource);
  }

  /**
   * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
   *
   * <p>For internal use to translate {@link GroupByKey}. For a large {@link PCollection} this
   * is expected to crash!
   *
   * <p>This is copied from the dataflow runner code.
   *
   * @param <T> the type of elements to concatenate.
   */
  private static class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {
    @Override
    public List<T> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
      List<T> result = createAccumulator();
      for (List<T> accumulator : accumulators) {
        result.addAll(accumulator);
      }
      return result;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }

    @Override
    public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(inputCoder);
    }
  }

  public void urnNotFound(
      String id, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    throw new IllegalArgumentException(
        String.format("Unknown type of URN %s for PTrasnform with id %s.",
            pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(),
            id));
  }

  private static void pruneOutput(
      DataSet<RawUnionValue> taggedDataset,
      BatchTranslationContext context,
      int unionTag,
      Coder<WindowedValue<Object>> outputCoder,
      String transformName,
      String collectionId) {
    TypeInformation<WindowedValue<Object>> outputType = new CoderTypeInformation<>(outputCoder);
    FlinkExecutableStagePruningFunction<Object> pruningFunction =
        new FlinkExecutableStagePruningFunction<>(unionTag);
    FlatMapOperator<RawUnionValue, WindowedValue<Object>> pruningOperator =
        new FlatMapOperator<>(taggedDataset, outputType, pruningFunction,
            String.format("%s/out.%d", transformName, unionTag));
    context.addDataSet(collectionId, pruningOperator);
  }

  // Creates a mapping from local names to output tag integers.
  private static BiMap<String, Integer> createOutputMap(Iterable<String> localOutputs) {
    ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
    int outputIndex = 0;
    for (String tag : localOutputs) {
      builder.put(tag, outputIndex);
      outputIndex++;
    }
    return builder.build();
  }

}
