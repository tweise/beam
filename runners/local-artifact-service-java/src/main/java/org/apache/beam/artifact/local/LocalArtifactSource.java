package org.apache.beam.artifact.local;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;

/**
 * An artifact source drawn from a local file system.
 */
public class LocalArtifactSource implements ArtifactSource {
  private static final int DEFAULT_CHUNK_SIZE_BYTES = 2 * 1024 * 1024;

  public static LocalArtifactSource create(LocalArtifactStagingLocation location) {
    return new LocalArtifactSource(location);
  }

  private final LocalArtifactStagingLocation location;

  private LocalArtifactSource(LocalArtifactStagingLocation location) {
    this.location = location;
  }

  @Override
  public ArtifactApi.Manifest getManifest() throws IOException {
    File manifestFile = location.getManifestFile();
    try (FileInputStream fileInputStream = new FileInputStream(manifestFile)) {
      return ArtifactApi.Manifest.parseFrom(fileInputStream);
    } catch (FileNotFoundException e) {
      return ArtifactApi.Manifest.getDefaultInstance();
    }
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactApi.ArtifactChunk> responseObserver) {
    File artifactFile = location.getArtifactFile(name);
    try (FileInputStream fStream = new FileInputStream(artifactFile)) {
      byte[] buffer = new byte[DEFAULT_CHUNK_SIZE_BYTES];
      for (int bytesRead = fStream.read(buffer); bytesRead > 0; bytesRead = fStream.read(buffer)) {
        ByteString data = ByteString.copyFrom(buffer, 0, bytesRead);
        responseObserver.onNext(ArtifactApi.ArtifactChunk.newBuilder().setData(data).build());
      }
      responseObserver.onCompleted();
    } catch (FileNotFoundException e) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(String.format("No such artifact %s", name))
              .withCause(e)
              .asException());
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  String.format("Could not retrieve artifact with name %s", name))
              .withCause(e)
              .asException());
    }
  }
}
