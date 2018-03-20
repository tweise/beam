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

package org.apache.beam.runners.flink.execution;

import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.state.StateDelegator;

/**
 * A handle to an {@link Environment} managed by a {@link SdkHarnessManager}.
 * Closing the session indicates to the session's {@link SdkHarnessManager}
 * that it can no longer use session resources such as its
 * {@link ArtifactSource}.
 */
public interface EnvironmentSession extends AutoCloseable {

  /**
   * Get the environment that the session uses.
   */
  Environment getEnvironment();

  /**
   * Get the ArtifactSource that the session uses.
   */
  ArtifactSource getArtifactSource();

  /**
   * Get an {@link SdkHarnessClient} that can communicate with an instance of the environment.
   */
  SdkHarnessClient getClient();

  /**
   * Get a {@link StateDelegator} with which to register a StateRequestHandler.
   */
  StateDelegator getStateDelegator();

  /**
   * Get a {@link ApiServiceDescriptor} for the data service.
   */
  ApiServiceDescriptor getDataServiceDescriptor();

  /**
   * Get a {@link ApiServiceDescriptor} for the state service.
   */
  ApiServiceDescriptor getStateServiceDescriptor();
}
