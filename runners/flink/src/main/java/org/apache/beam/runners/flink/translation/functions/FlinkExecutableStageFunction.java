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
package org.apache.beam.runners.flink.translation.functions;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Struct;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.execution.CachedArtifactSource;
import org.apache.beam.runners.flink.execution.EnvironmentSession;
import org.apache.beam.runners.flink.execution.SdkHarnessManager;
import org.apache.beam.runners.flink.execution.SingletonSdkHarnessManager;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.chaining.ExceptionInChainedStubException;
import org.apache.flink.util.Collector;

/** ExecutableStage operator. */
public class FlinkExecutableStageFunction<InputT> extends
    RichMapPartitionFunction<WindowedValue<InputT>, RawUnionValue> {

  private static final Logger logger =
      Logger.getLogger(FlinkExecutableStageFunction.class.getName());

  private final RunnerApi.ExecutableStagePayload payload;
  private final RunnerApi.Components components;
  private final RunnerApi.Environment environment;
  private final Map<String, Integer> outputMap;
  private final Struct pipelineOptions;

  private transient ExecutableStage executableStage;
  private transient EnvironmentSession session;
  private transient SdkHarnessClient client;
  private transient ExecutableProcessBundleDescriptor processBundleDescriptor;

  public FlinkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload payload,
      RunnerApi.Components components,
      RunnerApi.Environment environment,
      Struct pipelineOptions,
      Map<String, Integer> outputMap) {
    this.payload = payload;
    this.components = components;
    this.environment = environment;
    this.pipelineOptions = pipelineOptions;
    this.outputMap = outputMap;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    executableStage = ExecutableStage.fromPayload(payload, components);
    SdkHarnessManager manager = SingletonSdkHarnessManager.getInstance();
    ProvisionApi.ProvisionInfo provisionInfo = ProvisionApi.ProvisionInfo.newBuilder()
        // TODO: Set this from job metadata.
        .setJobId("job-id")
        .setWorkerId(getRuntimeContext().getTaskNameWithSubtasks())
        .setPipelineOptions(pipelineOptions)
        .build();
    ArtifactSource artifactSource =
        CachedArtifactSource.createDefault(getRuntimeContext().getDistributedCache());
    session = manager.getSession(provisionInfo, environment, artifactSource);
    Endpoints.ApiServiceDescriptor dataEndpoint = session.getDataServiceDescriptor();
    Endpoints.ApiServiceDescriptor stateEndpoint = session.getStateServiceDescriptor();
    client = session.getClient();
    logger.info(String.format("Data endpoint: %s", dataEndpoint.getUrl()));
    String id = new BigInteger(32, ThreadLocalRandom.current()).toString(36);
    processBundleDescriptor = ProcessBundleDescriptors.fromExecutableStage(
        id, executableStage, components, dataEndpoint, stateEndpoint);
    logger.info(String.format("Process bundle descriptor: %s", processBundleDescriptor));
  }

  @Override
  public void mapPartition(Iterable<WindowedValue<InputT>> input,
      Collector<RawUnionValue> collector) throws Exception {
    checkState(client != null, "SDK client not prepared");
    checkState(processBundleDescriptor != null,
        "ProcessBundleDescriptor not prepared");
    // NOTE: A double-cast is necessary below in order to hide pseudo-covariance from the compiler.
    @SuppressWarnings("unchecked")
    RemoteInputDestination<WindowedValue<InputT>> destination =
        (RemoteInputDestination<WindowedValue<InputT>>)
        (RemoteInputDestination<?>)
            processBundleDescriptor.getRemoteInputDestination();

    SdkHarnessClient.BundleProcessor<InputT> processor = client.getProcessor(
        processBundleDescriptor.getProcessBundleDescriptor(),
        destination,
        session.getStateDelegator());

    processor.getRegistrationFuture().toCompletableFuture().get();
    Map<BeamFnApi.Target, Coder<WindowedValue<?>>> outputCoders =
        processBundleDescriptor.getOutputTargetCoders();
    ImmutableMap.Builder<BeamFnApi.Target,
        SdkHarnessClient.RemoteOutputReceiver<?>> receiverBuilder = ImmutableMap.builder();
    final Object collectorLock = new Object();
    for (Map.Entry<BeamFnApi.Target, Coder<WindowedValue<?>>> entry : outputCoders.entrySet()) {
      BeamFnApi.Target target = entry.getKey();
      String writeNodeId = target.getPrimitiveTransformReference();
      RunnerApi.PTransform writeNode = processBundleDescriptor.getProcessBundleDescriptor()
          .getTransformsOrDefault(writeNodeId, null);
      checkArgument(writeNode != null,
          "Could not find write node for id %s", writeNodeId);
      String collectionId = Iterables.getOnlyElement(writeNode.getInputsMap().values());
      Integer boxedUnionTag = outputMap.get(collectionId);
      checkArgument(boxedUnionTag != null,
          "No union tag found for PCollection %d", collectionId);
      int unionTag = boxedUnionTag;
      Coder<WindowedValue<?>> coder = entry.getValue();
      SdkHarnessClient.RemoteOutputReceiver<WindowedValue<?>> receiver =
          new SdkHarnessClient.RemoteOutputReceiver<WindowedValue<?>>() {
            @Override
            public Coder<WindowedValue<?>> getCoder() {
              return coder;
            }

            @Override
            public FnDataReceiver<WindowedValue<?>> getReceiver() {
              return new FnDataReceiver<WindowedValue<?>>() {
                @Override
                public void accept(WindowedValue<?> input) throws Exception {
                  logger.finer(String.format("Receiving value: %s", input));
                  // TODO: Can this be called by multiple threads? Are calls guaranteed to at least
                  // be serial? If not, these calls may need to be synchronized.
                  // TODO: If this needs to be synchronized, consider requiring immutable maps.
                  synchronized (collectorLock) {
                    try {
                      collector.collect(new RawUnionValue(unionTag, input));
                    } catch (ExceptionInChainedStubException e) {
                      // Flink wraps exceptions that happen in certain places, make sure
                      // to bubble up the right exception
                      throw e.getWrappedException();
                    }
                  }
                }
              };
            }
          };
      receiverBuilder.put(target, receiver);
    }
    Map<BeamFnApi.Target,
        SdkHarnessClient.RemoteOutputReceiver<?>> receiverMap =
        receiverBuilder.build();

    StateRequestHandler stateRequestHandler =
        FlinkBatchStateRequestHandler.forStage(executableStage, components, getRuntimeContext());

    try (SdkHarnessClient.ActiveBundle<InputT> bundle =
        processor.newBundle(receiverMap, stateRequestHandler)) {
      FnDataReceiver<WindowedValue<InputT>> inputReceiver = bundle.getInputReceiver();
      for (WindowedValue<InputT> value : input) {
        logger.finer(String.format("Sending value: %s", value));
        inputReceiver.accept(value);
      }
    }

  }

  @Override
  public void close() throws Exception {
    // TODO: In what order should the client and session be closed? Should the session eventually
    // own the client itself?
    client.close();
    client = null;
    session.close();
  }

}
