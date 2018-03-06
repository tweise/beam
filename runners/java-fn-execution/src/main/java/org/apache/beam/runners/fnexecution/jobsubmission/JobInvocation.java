package org.apache.beam.runners.fnexecution.jobsubmission;

import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobMessage;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;

/**
 * Internal representation of a Job which has been invoked (prepared and run) by a client.
 */
public interface JobInvocation {

  /**
   * Start the job.
   */
  void start();

  /**
   * @return Unique identifier for the job invocation.
   */
  String getId();

  /**
   * Cancel the job.
   */
  void cancel();

  /**
   * Retrieve the job's current state.
   */
  JobState.Enum getState();

  /**
   * Observe job state changes with an observer.
   */
  void addStateObserver(StreamObserver<JobState.Enum> stateStreamObserver);

  /**
   * Observe job messages with an observer.
   */
  void addMessageObserver(StreamObserver<JobMessage> messageStreamObserver);
}
