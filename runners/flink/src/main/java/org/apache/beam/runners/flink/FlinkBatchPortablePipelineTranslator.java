package org.apache.beam.runners.flink;

import akka.dispatch.BatchingExecutor;
import com.google.common.collect.Iterables;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.QueryablePipeline;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class FlinkBatchPortablePipelineTranslator implements FlinkPortablePipelineTranslator<FlinkBatchPortablePipelineTranslator.BatchTranslationContext> {

  // Shared between batch and streaming
  interface TranslationContext {
    FlinkPipelineExecutionEnvironment getFlinkPipelineExecutionEnvironment();
    <T> void addDataSet(String id, DataSet<T> dataSet);
    <T> DataSet<T> getDataSetOrThrow(String id);
  }

  // For batch only
  interface BatchTranslationContext extends TranslationContext {
    ExecutionEnvironment getExecutionEnvironment();
  }

  private abstract class TranslationContextImpl {
    private final Map<String, DataSet<?>> dataSets;
    private final FlinkPipelineExecutionEnvironment env;

    protected TranslationContextImpl(FlinkPipelineExecutionEnvironment env) {
      this.env = env;
      dataSets = new HashMap<>();
    }

    public <T> void addDataSet(String id, DataSet<T> dataSet) {
      dataSets.put(id, dataSet);
    }

    public <T> DataSet<T> getDataSetOrThrow(String id) {
      DataSet<T> dataSet = (DataSet<T>) dataSets.get(id);
      if (dataSet == null) {
        throw new IllegalArgumentException(
            String.format("Unknown dataset for id %s.", id));
      }
      return dataSet;
    }
  }

  private class BatchTranslationContextImpl extends TranslationContextImpl {
    private final ExecutionEnvironment env;
    private BatchTranslationContextImpl(
        FlinkPipelineExecutionEnvironment flinkPipelineExecutionEnvironment,
        ExecutionEnvironment environment) {
      super(flinkPipelineExecutionEnvironment);
      this.env = environment;
    }
  }

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  private final Map<String, PTransformTranslator<BatchTranslationContext>>
      urnToTransformTranslator = new HashMap<>();

  FlinkBatchPortablePipelineTranslator() {
    urnToTransformTranslator.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, this::translateFlatten);
  }

  @Override
  public void translate(
      BatchTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator.getOrDefault(
          transform.getTransform().getSpec().getUrn(), this::urnNotFound)
          .translate(transform.getId(), pipeline, context);
    }
  }

  public <T> void translateFlatten(
      String id,
      RunnerApi.Pipeline pipeline,
      BatchTranslationContext context) {
    Map<String, String> allInputs = pipeline.getComponents().getTransformsOrThrow(id).getInputsMap();
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

  public <T> void urnNotFound(String id, RunnerApi.Pipeline pipeline, BatchTranslationContext context) {
    throw new IllegalArgumentException(
        String.format("Unknown type of URN %s for PTrasnform with id %s.",
            pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(),
            id));
  }
}
