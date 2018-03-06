package org.apache.beam.runners.fnexecution.jobsubmission;

import io.grpc.stub.StreamObserver;
import java.util.function.Function;

/**
 * Utility class that transforms observations into one that an output observer can accept.
 *
 * <p>This class synchronizes on outputObserver when handling it, allowing for multiple
 * TransformStreamObservers to multiplex to a single outputObserver.
 *
 * @param <T1> Input Type
 * @param <T2> Output Type
 */
class TransformStreamObserver<T1, T2> implements StreamObserver<T1> {

  /**
   * Create a new TransformStreamObserver.
   *
   * @param transform The function used to transform observations.
   * @param outputObserver The observer to forward transform outputs to.
   */
  public static <T1, T2> TransformStreamObserver<T1, T2> create(
      Function<T1, T2> transform, StreamObserver<T2> outputObserver) {
    return new TransformStreamObserver<>(transform, outputObserver);
  }

  private final Function<T1, T2> transform;
  private final StreamObserver<T2> outputObserver;

  private TransformStreamObserver(Function<T1, T2> transform, StreamObserver<T2> outputObserver) {
    this.transform = transform;
    this.outputObserver = outputObserver;
  }

  @Override
  public void onNext(T1 i) {
    T2 o = transform.apply(i);
    synchronized (outputObserver) {
      outputObserver.onNext(o);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    synchronized (outputObserver) {
      outputObserver.onError(throwable);
    }
  }

  @Override
  public void onCompleted() {
    synchronized (outputObserver) {
      outputObserver.onCompleted();
    }
  }
}

