package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/** Flink function that demultiplexes output from a multi-output executable stage. */
public class FlinkExecutableStagePruningFunction<T>
    implements FlatMapFunction<RawUnionValue, WindowedValue<T>> {

  private final int unionTag;

  public FlinkExecutableStagePruningFunction(int unionTag) {
    this.unionTag = unionTag;
  }

  @Override
  public void flatMap(RawUnionValue rawUnionValue, Collector<WindowedValue<T>> collector) {
    if (rawUnionValue.getUnionTag() == unionTag) {
      collector.collect((WindowedValue<T>) rawUnionValue.getValue());
    }
  }
}
