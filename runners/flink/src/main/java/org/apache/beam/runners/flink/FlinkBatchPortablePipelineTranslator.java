package org.apache.beam.runners.flink;

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
import org.apache.flink.api.java.operators.DataSource;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class FlinkBatchPortablePipelineTranslator implements FlinkPortablePipelineTranslator {

  interface PTransformTranslator<T> {
    void translate(String id, RunnerApi.Pipeline pipeline, T t);
  }

  private final Map<String, PTransformTranslator<FlinkPipelineExecutionEnvironment>>
      urnToTransformTranslator = new HashMap<>();

  FlinkBatchPortablePipelineTranslator() {
    urnToTransformTranslator.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, this::translateFlatten);
  }

  @Override
  public void translate(
      FlinkPipelineExecutionEnvironment environment, RunnerApi.Pipeline pipeline) {
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(pipeline.getComponents());
    for (PipelineNode.PTransformNode transform : p.getTopologicallyOrderedTransforms()) {
      urnToTransformTranslator.getOrDefault(
          transform.getTransform().getSpec().getUrn(), this::urnNotFound)
          .translate(transform.getId(), pipeline, environment);
    }
  }

  public <T> void translateFlatten(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkPipelineExecutionEnvironment env) {

  }

  public <T> void urnNotFound(String id, RunnerApi.Pipeline pipeline, T t) {
    throw new IllegalArgumentException(
        String.format("Unknown type of URN %s for PTrasnform with id %s.",
            pipeline.getComponents().getTransformsOrThrow(id).getSpec().getUrn(),
            id));
  }
}
