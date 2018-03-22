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
package org.apache.beam.runners.fnexecution.environment;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.util.ThrowingSupplier;

/** An {@link EnvironmentManager} that manages a single docker container. Not thread-safe. */
public class SingletonDockerEnvironmentManager implements EnvironmentManager {

  private static final Logger logger =
      Logger.getLogger(SingletonDockerEnvironmentManager.class.getName());

  public static SingletonDockerEnvironmentManager forServices(
      DockerWrapper docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ThrowingSupplier<InstructionRequestHandler> requestHandler) {
    return new SingletonDockerEnvironmentManager(docker, controlServiceServer, loggingServiceServer,
        retrievalServiceServer, provisioningServiceServer, requestHandler);
  }

  private final DockerWrapper docker;
  private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
  private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
  private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
  private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
  private final ThrowingSupplier<InstructionRequestHandler> requestHandler;

  private RemoteEnvironment dockerEnvironment = null;

  private SingletonDockerEnvironmentManager(
      DockerWrapper docker,
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ThrowingSupplier<InstructionRequestHandler> requestHandler) {
    this.docker = docker;
    this.controlServiceServer = controlServiceServer;
    this.loggingServiceServer = loggingServiceServer;
    this.retrievalServiceServer = retrievalServiceServer;
    this.provisioningServiceServer = provisioningServiceServer;
    this.requestHandler = requestHandler;
  }

  /**
   * Retrieve a handle for the given environment. The same environment must be requested every time.
   * The same remote handle is returned to every caller, so the environment cannot be used once
   * closed.
   */
  @Override
  public RemoteEnvironment getEnvironment(Environment environment) throws Exception {
    if (dockerEnvironment == null) {
      dockerEnvironment = createDockerEnv(environment);
    } else {
      checkArgument(
          environment.getUrl().equals(dockerEnvironment.getEnvironment().getUrl()),
          "A %s must only be queried for a single %s. Existing %s, Argument %s",
          SingletonDockerEnvironmentManager.class.getSimpleName(),
          Environment.class.getSimpleName(),
          dockerEnvironment.getEnvironment().getUrl(),
          environment.getUrl());
    }
    return dockerEnvironment;
  }

  private DockerContainerEnvironment createDockerEnv(Environment environment) throws Exception {
    // TODO: Generate environment id correctly.
    String environmentId = Long.toString(-123);
    Path workerPersistentDirectory = Files.createTempDirectory(Paths.get("/tmp"),
            "worker_persistent_directory");
    Path semiPersistentDirectory = Files.createTempDirectory(Paths.get("/tmp"),
            "semi_persistent_dir");
    //Path semiPersistentDirectory = Paths.get("/tmp");
    String containerImage = environment.getUrl();
    // TODO: The default service address will not work for Docker for Mac.
    String loggingEndpoint = loggingServiceServer.getApiServiceDescriptor().getUrl();
    String artifactEndpoint = retrievalServiceServer.getApiServiceDescriptor().getUrl();
    String provisionEndpoint = provisioningServiceServer.getApiServiceDescriptor().getUrl();
    String controlEndpoint = controlServiceServer.getApiServiceDescriptor().getUrl();

    logger.info(String.format("Logging endpoint: %s", loggingEndpoint));
    logger.info(String.format("Artifact endpoint: %s", artifactEndpoint));
    logger.info(String.format("Provision endpoint: %s", provisionEndpoint));
    logger.info(String.format("Control endpoint: %s", controlEndpoint));
    List<String> dockerArgs = Arrays.asList(
        "-v",
        String.format("%s:%s", workerPersistentDirectory, semiPersistentDirectory),
        // NOTE: Host networking does not actually work for Mac.
        "--network=host");
    List<String> sdkHarnessArgs = Arrays.asList(
        String.format("--id=%s", environmentId),
        String.format("--logging_endpoint=%s", loggingEndpoint),
        String.format("--artifact_endpoint=%s", artifactEndpoint),
        String.format("--provision_endpoint=%s", provisionEndpoint),
        String.format("--control_endpoint=%s", controlEndpoint),
        String.format("--semi_persist_dir=%s", semiPersistentDirectory));
    String containerId = docker.runImage(containerImage, dockerArgs, sdkHarnessArgs);
    System.out.println("GOT ID: " + containerId);
    return DockerContainerEnvironment.create(docker, environment, containerId,
        requestHandler.get());
  }

}
