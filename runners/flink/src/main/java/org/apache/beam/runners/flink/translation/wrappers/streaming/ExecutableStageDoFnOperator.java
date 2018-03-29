package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.flink.util.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Struct;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.flink.execution.CachedArtifactSource;
import org.apache.beam.runners.flink.execution.EnvironmentSession;
import org.apache.beam.runners.flink.execution.SdkHarnessManager;
import org.apache.beam.runners.flink.execution.SingletonSdkHarnessManager;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;


/**
 * ExecutableStageDoFnOperator.
 * SDK harness interaction code adopted from
 * {@link org.apache.beam.runners.flink.translation.functions.FlinkExecutableStageFunction}.
 * Reuse of {@link DoFnOperator} for windowing/watermark support.
 * @param <InputT>
 * @param <OutputT>
 */
public class ExecutableStageDoFnOperator<InputT, OutputT> extends DoFnOperator<InputT, OutputT> {

  private static final Logger logger =
          Logger.getLogger(ExecutableStageDoFnOperator.class.getName());

  private final RunnerApi.ExecutableStagePayload payload;
  private final RunnerApi.Components components;
  private final RunnerApi.Environment environment;

  private transient EnvironmentSession session;
  private transient SdkHarnessClient client;
  private transient ProcessBundleDescriptors.ExecutableProcessBundleDescriptor
          processBundleDescriptor;

  public ExecutableStageDoFnOperator(String stepName,
                                     Coder<WindowedValue<InputT>> inputCoder,
                                     TupleTag<OutputT> mainOutputTag,
                                     List<TupleTag<?>> additionalOutputTags,
                                     OutputManagerFactory<OutputT> outputManagerFactory,
                                     WindowingStrategy<?, ?> windowingStrategy,
                                     Map<Integer, PCollectionView<?>> sideInputTagMapping,
                                     Collection<PCollectionView<?>> sideInputs,
                                     PipelineOptions options,
                                     Coder<?> keyCoder,
                                     RunnerApi.ExecutableStagePayload payload,
                                     RunnerApi.Components components,
                                     RunnerApi.Environment environment
                                     ) {
    super(new NoOpDoFn(),
            stepName, inputCoder, mainOutputTag, additionalOutputTags,
            outputManagerFactory, windowingStrategy,
            sideInputTagMapping, sideInputs, options, keyCoder);
      this.payload = payload;
      this.components = components;
      this.environment = environment;
  }

  @Override
  public void open() throws Exception {
    super.open();

    ExecutableStage stage = ExecutableStage.fromPayload(payload, components);
    SdkHarnessManager manager = SingletonSdkHarnessManager.getInstance();
    Struct options = PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create());
    ProvisionApi.ProvisionInfo provisionInfo = ProvisionApi.ProvisionInfo.newBuilder()
            // TODO: Set this from job metadata.
            .setJobId("job-id")
            .setWorkerId(getRuntimeContext().getTaskNameWithSubtasks())
            .setPipelineOptions(options)
            .build();
    ArtifactSource artifactSource =
            CachedArtifactSource.createDefault(getRuntimeContext().getDistributedCache());
    session = manager.getSession(provisionInfo, environment, artifactSource);
    Endpoints.ApiServiceDescriptor dataEndpoint = session.getDataServiceDescriptor();
    client = session.getClient();
    logger.info(String.format("Data endpoint: %s", dataEndpoint.getUrl()));
    String id = new BigInteger(32, ThreadLocalRandom.current()).toString(36);
    processBundleDescriptor =
            ProcessBundleDescriptors.fromExecutableStage(id, stage, components, dataEndpoint);
    logger.info(String.format("Process bundle descriptor: %s", processBundleDescriptor));
  }

  // TODO: currently assumes that every element is a separate bundle,
  // but this can be changed by pushing some of this logic into the "DoFnRunner"
  private void processElementWithSdkHarness(WindowedValue<InputT> element) throws Exception {

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
            processBundleDescriptor.getProcessBundleDescriptor(), destination);
    processor.getRegistrationFuture().toCompletableFuture().get();
    Map<BeamFnApi.Target, Coder<WindowedValue<?>>> outputCoders =
            processBundleDescriptor.getOutputTargetCoders();
    ImmutableMap.Builder<BeamFnApi.Target,
            SdkHarnessClient.RemoteOutputReceiver<?>> receiverBuilder = ImmutableMap.builder();
    final Object collectorLock = new Object();
    for (Map.Entry<BeamFnApi.Target, Coder<WindowedValue<?>>> entry : outputCoders.entrySet()) {
      BeamFnApi.Target target = entry.getKey();
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
                      // TODO: Can this be called by multiple threads? Are calls guaranteed to
                      // at least
                      // be serial? If not, these calls may need to be synchronized.
                      // TODO: If this needs to be synchronized, consider requiring immutable maps.
                      synchronized (collectorLock) {
                        // TODO: Get the correct output union tag from the corresponding output tag.
                        // Plumb through output tags from process bundle descriptor.
                        // TODO: Plumb through TupleTag <-> Target mappings to get correct union tag
                        outputManager.output(mainOutputTag, (WindowedValue) input);
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

    try (SdkHarnessClient.ActiveBundle<InputT> bundle = processor.newBundle(receiverMap)) {
      FnDataReceiver<WindowedValue<InputT>> inputReceiver = bundle.getInputReceiver();
      logger.finer(String.format("Sending value: %s", element));
      inputReceiver.accept(element);
    }

  }

  @Override
  public void close() throws Exception {
    // TODO: In what order should the client and session be closed? Should the session eventually
    // own the client itself?
    client.close();
    client = null;
    session.close();

    super.close();
  }

  // TODO: remove single element bundle assumption
  @Override
  protected DoFnRunner<InputT, OutputT> createWrappingDoFnRunner(
          DoFnRunner<InputT, OutputT> wrappedRunner) {
    return new SdkHarnessDoFunRunner();
  }

  private class SdkHarnessDoFunRunner implements DoFnRunner<InputT, OutputT> {
    @Override
    public void startBundle() {

    }

    @Override
    public void processElement(WindowedValue<InputT> elem) {
      try {
        processElementWithSdkHarness(elem);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onTimer(String timerId, BoundedWindow window, Instant timestamp,
                        TimeDomain timeDomain) {

    }

    @Override
    public void finishBundle() {

    }
  }

  private static class NoOpDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
    @ProcessElement
    public void doNothing(ProcessContext context) {}
  }

}
