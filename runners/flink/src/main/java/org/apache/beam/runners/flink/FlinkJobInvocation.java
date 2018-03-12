package org.apache.beam.runners.flink;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invocation of a Flink Job via {@link FlinkRunner}.
 */
public class FlinkJobInvocation implements JobInvocation {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkJobInvocation.class);

  public static FlinkJobInvocation create(
      String id,
      ListeningExecutorService executorService,
      FlinkRunner runner, Pipeline pipeline) {
    return new FlinkJobInvocation(id, executorService, runner, pipeline);
  }

  private final String id;
  private final ListeningExecutorService executorService;
  private final FlinkRunner runner;
  private final Pipeline pipeline;
  private Enum jobState;
  private List<StreamObserver<Enum>> stateObservers;

  @Nullable
  private ListenableFuture<PipelineResult> invocationFuture;

  private FlinkJobInvocation(
      String id,
      ListeningExecutorService executorService,
      FlinkRunner runner,
      Pipeline pipeline) {
    this.id = id;
    this.executorService = executorService;
    this.runner = runner;
    this.pipeline = pipeline;
    this.invocationFuture = null;
    this.jobState = Enum.STOPPED;
    this.stateObservers = new ArrayList<>();
  }

  @Override
  public void start() {
    LOG.trace("Starting job invocation {}", getId());
    synchronized (this) {
      setState(Enum.STARTING);
      invocationFuture = executorService.submit(() -> runner.run(pipeline));
      setState(Enum.RUNNING);
      Futures.addCallback(
          invocationFuture,
          new FutureCallback<PipelineResult>() {
            @Override
            public void onSuccess(@Nullable PipelineResult pipelineResult) {
              setState(Enum.DONE);
            }

            @Override
            public void onFailure(Throwable throwable) {
              String message = String.format("Error during job invocation %s.", getId());
              LOG.error(message, throwable);
              setState(Enum.FAILED);
            }
          },
          executorService
      );
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void cancel() {
    LOG.trace("Canceling job invocation {}", getId());
    synchronized (this) {
      if (this.invocationFuture != null) {
        this.invocationFuture.cancel(true /* mayInterruptIfRunning */);
      }
    }
  }

  @Override
  public synchronized Enum getState() {
      return this.jobState;
  }

  @Override
  public synchronized void addStateObserver(StreamObserver<Enum> stateStreamObserver) {
    stateStreamObserver.onNext(getState());
    stateObservers.add(stateStreamObserver);
  }

  @Override
  public synchronized void addMessageObserver(StreamObserver<JobMessage> messageStreamObserver) {
    LOG.warn("addMessageObserver() not yet implemented.");
  }

  private synchronized void setState(Enum state) {
    this.jobState = state;
    for (StreamObserver<Enum> observer : stateObservers) {
      observer.onNext(state);
    }
  }
}
