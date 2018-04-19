package org.apache.beam.runners.flink;

import com.google.common.collect.Iterables;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.PTransformTranslator;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.util.Map;
import java.util.Properties;

public class CustomFlinkStreamingPortableTranslations {

  public void addTo(Map<String, PTransformTranslator<StreamingTranslationContext>> urnToTransformTranslator) {
    urnToTransformTranslator.put("custom:kafkaInput", this::translateKafkaInput);
  }

  private void translateKafkaInput(
          String id,
          RunnerApi.Pipeline pipeline,
          FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform =
            pipeline.getComponents().getTransformsOrThrow(id);

    //if (true) {
    //  throw new UnsupportedOperationException(String.format("not implemented: id=%s, transform=%s", id, pTransform));
    //}

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "beam-example-group");

    DataStreamSource<WindowedValue<byte[]>> source = context.getExecutionEnvironment().addSource(
            new FlinkKafkaConsumer010<>("beam-example",
                    new ByteArrayWindowedValueSchema(), properties).setStartFromLatest()
            );
    context.addDataStream(
            Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
            source);
  }

  /**
   * Deserializer for native Flink Kafka source that produces {@link WindowedValue} expected by Beam operators.
   */
  private static class ByteArrayWindowedValueSchema implements KeyedDeserializationSchema<WindowedValue<byte[]>> {
    private static final long serialVersionUID = -1L;

    private final TypeInformation<WindowedValue<byte[]>> ti;
    long cnt = 0;

    public ByteArrayWindowedValueSchema() {
      this.ti = new CoderTypeInformation<>(WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));
    }

    @Override
    public TypeInformation<WindowedValue<byte[]>> getProducedType() {
      return ti;
    }

    @Override
    public WindowedValue<byte[]> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
      cnt++;
      System.out.println("###Kafka record: " + new String(message));
      return WindowedValue.valueInGlobalWindow(message);
    }

    @Override
    public boolean isEndOfStream(WindowedValue<byte[]> nextElement) {
      return false;
    }
  }

}
