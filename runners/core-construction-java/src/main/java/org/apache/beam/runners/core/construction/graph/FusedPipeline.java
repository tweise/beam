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

package org.apache.beam.runners.core.construction.graph;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/** A {@link Pipeline} which has been separated into collections of executable components. */
@AutoValue
public abstract class FusedPipeline {
  static FusedPipeline of(
      Set<ExecutableStage> environmentalStages, Set<PTransformNode> runnerStages) {
    return new AutoValue_FusedPipeline(environmentalStages, runnerStages);
  }

  /** The {@link ExecutableStage executable stages} that are executed by SDK harnesses. */
  public abstract Set<ExecutableStage> getFusedStages();

  /** The {@link PTransform PTransforms} that a runner is responsible for executing. */
  public abstract Set<PTransformNode> getRunnerExecutedTransforms();

  public RunnerApi.Pipeline toPipeline(Components initialComponents) {
    Map<String, PTransform> executableTransforms = getExecutableTransforms(initialComponents);
    Components fusedComponents = initialComponents.toBuilder()
        .putAllTransforms(executableTransforms)
        .putAllTransforms(getFusedTransforms())
        .build();
    List<String> rootTransformIds =
        StreamSupport.stream(
                QueryablePipeline.forTransforms(executableTransforms.keySet(), fusedComponents)
                    .getTopologicallyOrderedTransforms()
                    .spliterator(),
                false)
            .map(PTransformNode::getId)
            .collect(Collectors.toList());
    return Pipeline.newBuilder()
        .setComponents(fusedComponents)
        .addAllRootTransformIds(rootTransformIds)
        .build();
  }

  /**
   * Return a {@link Components} like the {@code base} components, but with the set of transforms to
   * be executed by the runner.
   *
   * <p>The transforms that are present in the returned map are the union of the results of {@link
   * #getRunnerExecutedTransforms()} and {@link #getFusedStages()}, where each {@link
   * ExecutableStage}.
   */
  private Map<String, PTransform> getExecutableTransforms(Components base) {
    Map<String, PTransform> topLevelTransforms = new HashMap<>();
    for (PTransformNode runnerExecuted : getRunnerExecutedTransforms()) {
      topLevelTransforms.put(runnerExecuted.getId(), runnerExecuted.getTransform());
    }
    for (ExecutableStage stage : getFusedStages()) {
      topLevelTransforms.put(
          generateStageId(
              stage, Sets.union(base.getTransformsMap().keySet(), topLevelTransforms.keySet())),
          stage.toPTransform());
    }
    return topLevelTransforms;
  }

  private Map<String, PTransform> getFusedTransforms() {
    return getFusedStages()
        .stream()
        .flatMap(stage -> stage.getTransforms().stream())
        .collect(Collectors.toMap(PTransformNode::getId, PTransformNode::getTransform));
  }

  private String generateStageId(ExecutableStage stage, Set<String> existingIds) {
    int i = 0;
    String name;
    do {
      // Instead this could include the name of the root transforms
      name =
          String.format(
              "%s/%s.%s",
              stage.getInputPCollection().getPCollection().getUniqueName(),
              stage.getEnvironment().getUrl(),
              i);
      i++;
    } while (existingIds.contains(name));
    return name;
  }
}
