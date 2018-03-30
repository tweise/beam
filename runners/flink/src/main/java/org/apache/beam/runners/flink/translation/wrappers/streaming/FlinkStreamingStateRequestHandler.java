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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.OperatorStateBackend;

/**
 * {@link StateRequestHandler} that uses a Flink {@link OperatorStateBackend} to manage state.
 */
class FlinkStreamingStateRequestHandler implements StateRequestHandler {

  private final RunnerApi.ExecutableStagePayload payload;
  private final RunnerApi.Components components;
  private final RuntimeContext runtimeContext;

  public FlinkStreamingStateRequestHandler(
          RunnerApi.ExecutableStagePayload payload,
          RunnerApi.Components components, RuntimeContext runtimeContext) {
    this.payload = payload;
    this.components = components;
    this.runtimeContext = runtimeContext;
  }

  @Override
  public CompletionStage<BeamFnApi.StateResponse.Builder> handle(
          BeamFnApi.StateRequest request) throws Exception {
    throw new UnsupportedOperationException();
  }

}
