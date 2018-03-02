package org.apache.beam.runners.flink;

/**
 * Determines artifact path names within the
 * {@link org.apache.flink.api.common.cache.DistributedCache}.
 */
public class FlinkCachedArtifactPaths {
  private static final String DEFAULT_ARTIFACT_TOKEN = "default";

  public static FlinkCachedArtifactPaths createDefault() {
    return new FlinkCachedArtifactPaths(DEFAULT_ARTIFACT_TOKEN);
  }

  public static FlinkCachedArtifactPaths forToken(String artifactToken) {
    return new FlinkCachedArtifactPaths(artifactToken);
  }

  private final String token;

  private FlinkCachedArtifactPaths(String token) {
    this.token = token;
  }

  public String getArtifactPath(String name) {
    return String.format("ARTIFACT_%s_%s", token, name);
  }

  public String getManifestPath() {
    return String.format("MANIFEST_%s", token);
  }
}
