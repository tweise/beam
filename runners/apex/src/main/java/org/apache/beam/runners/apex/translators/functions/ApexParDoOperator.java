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

package org.apache.beam.runners.apex.translators.functions;

import java.util.Collection;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translators.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translators.utils.NoOpStepContext;
import org.apache.beam.runners.apex.translators.utils.SerializablePipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AggregatorRetriever;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.DoFnRunner;
import org.apache.beam.sdk.util.DoFnRunners;
import org.apache.beam.sdk.util.DoFnRunners.OutputManager;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.common.Counter;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

/**
 * Apex operator for Beam {@link DoFn}.
 */
public class ApexParDoOperator<InputT, OutputT> extends BaseOperator implements OutputManager {

  private transient final TupleTag<OutputT> mainTag = new TupleTag<OutputT>();
  private transient DoFnRunner<InputT, OutputT> doFnRunner;

  @Bind(JavaSerializer.class)
  private final SerializablePipelineOptions pipelineOptions;
  @Bind(JavaSerializer.class)
  private final DoFn<InputT, OutputT> doFn;
  @Bind(JavaSerializer.class)
  private final WindowingStrategy<?, ?> windowingStrategy;
  @Bind(JavaSerializer.class)
  private final SideInputReader sideInputReader;

  public ApexParDoOperator(
      ApexPipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      SideInputReader sideInputReader) {
    this.pipelineOptions = new SerializablePipelineOptions(pipelineOptions);
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputReader = sideInputReader;
  }

  @SuppressWarnings("unused") // for Kryo
  private ApexParDoOperator() {
    this(null, null, null, null);
  }

  public final transient DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>> input = new DefaultInputPort<ApexStreamTuple<WindowedValue<InputT>>>()
  {
    @Override
    public void process(ApexStreamTuple<WindowedValue<InputT>> t)
    {
      if (t instanceof ApexStreamTuple.WatermarkTuple) {
        output.emit(t);
      } else {
        System.out.println("\n" + Thread.currentThread().getName() + "\n" + t.getValue() + "\n");
        doFnRunner.processElement(t.getValue());
      }
    }
  };

  @OutputPortFieldAnnotation(optional=true)
  public final transient DefaultOutputPort<ApexStreamTuple<?>> output = new DefaultOutputPort<>();

  @Override
  public <T> void output(TupleTag<T> tag, WindowedValue<T> tuple)
  {
    output.emit(ApexStreamTuple.DataTuple.of(tuple));
  }

  private transient CounterSet counterSet;

  @Override
  public void setup(OperatorContext context)
  {
    this.counterSet = new CounterSet() {
      @Override
      public boolean addCounter(Counter<?> counter) {
        System.out.println("\n\nCounter: " + counter);
        return add(counter);
      }
    };
    this.doFnRunner = DoFnRunners.simpleRunner(pipelineOptions.get(),
        doFn,
        sideInputReader,
        this,
        mainTag,
        TupleTagList.empty().getAll(),
        new NoOpStepContext(),
        this.counterSet.getAddCounterMutator(),
        windowingStrategy
        );
  }

  @Override
  public void beginWindow(long windowId)
  {
    doFnRunner.startBundle();
    /*
    Collection<Aggregator<?, ?>> aggregators = AggregatorRetriever.getAggregators(doFn);
    if (!aggregators.isEmpty()) {
      System.out.println("\n" + Thread.currentThread().getName() + "\n" +AggregatorRetriever.getAggregators(doFn) + "\n");
    }
    */
  }

  @Override
  public void endWindow()
  {
    doFnRunner.finishBundle();
  }
}
