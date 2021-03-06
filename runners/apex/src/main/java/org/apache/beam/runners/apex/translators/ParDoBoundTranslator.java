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

package org.apache.beam.runners.apex.translators;

import org.apache.beam.runners.apex.translators.functions.ApexParDoOperator;
import org.apache.beam.runners.apex.translators.utils.NoOpSideInputReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link ParDo.Bound} is translated to Apex operator that wraps the {@link DoFn}
 */
public class ParDoBoundTranslator<InputT, OutputT> implements
    TransformTranslator<ParDo.Bound<InputT, OutputT>> {
  private static final long serialVersionUID = 1L;

  @Override
  public void translate(ParDo.Bound<InputT, OutputT> transform, TranslationContext context) {
    DoFn<InputT, OutputT> doFn = transform.getFn();
    PCollection<OutputT> output = context.getOutput();
    ApexParDoOperator<InputT, OutputT> operator = new ApexParDoOperator<>(context.getPipelineOptions(),
        doFn, output.getWindowingStrategy(), new NoOpSideInputReader());
    context.addOperator(operator, operator.output);
    context.addStream(context.getInput(), operator.input);
  }
}
