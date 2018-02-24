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

import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;

/**
 * A long-lived class to manage SDK harness container instances on behalf of
 * shorter-lived Flink operators.
 */
public interface SdkHarnessManager {
  /**
   * Get a new {@link EnvironmentSession} to an {@link Environment} container
   * instance, creating a new instance if necessary.
   *
   * @param environment The environment specification for the desired session.
   * @param artifactSource An artifact source that can be used during creation.
   */
  EnvironmentSession getSession(
      ProvisionApi.ProvisionInfo jobInfo,
      Environment environment,
      ArtifactSource artifactSource) throws Exception;
}
