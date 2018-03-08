package org.apache.beam.runners.fnexecution.artifact;


import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link ArtifactRetrievalService} implemented via gRPC.
 *
 * <p>Relies on one or more {@link ArtifactSource} for artifact retrieval.
 */
public class GrpcArtifactProxyService
    extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements FnService, ArtifactRetrievalService {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcArtifactProxyService.class);

  public static GrpcArtifactProxyService fromSource(ArtifactSource artifactSource) {
    return new GrpcArtifactProxyService(artifactSource);
  }

  private final ArtifactSource artifactSource;

  private GrpcArtifactProxyService(ArtifactSource artifactSource) {
    this.artifactSource = artifactSource;
  }

  @Override
  public void getManifest(
      ArtifactApi.GetManifestRequest request,
      StreamObserver<ArtifactApi.GetManifestResponse> responseObserver) {
    try {
      ArtifactApi.Manifest manifest = artifactSource.getManifest();
      ArtifactApi.GetManifestResponse response =
          ArtifactApi.GetManifestResponse.newBuilder().setManifest(manifest).build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOGGER.warn("Error retrieving manifest", e);
      responseObserver.onError(
          Status.INTERNAL
              .withDescription("Could not retrieve manifest.")
              .withCause(e)
              .asException());
    }
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    artifactSource.getArtifact(request.getName(), responseObserver);
  }

  @Override
  public void close() throws Exception {
    // TODO: wrap up any open streams
    LOGGER.warn("GrpcArtifactProxyService.close() was called but is not yet implemented.");
  }
}
