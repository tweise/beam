import logging
import json
import sys
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import universal_local_runner

from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.core import Windowing
from apache_beam import pvalue

from google.protobuf import wrappers_pb2


class CustomKafkaInput(PTransform):
    """Custom transform primitive."""
    consumer_properties = {'bootstrap.servers' : 'localhost:9092'}

    def expand(self, pbegin):
        assert isinstance(pbegin, pvalue.PBegin), (
                'Input to transform must be a PBegin but found %s' % pbegin)
        return pvalue.PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
        return Windowing(GlobalWindows())

    def infer_output_type(self, unused_input_type):
        return bytes

    #TODO master has fixed the case where typed_param is already a string
    def to_runner_api(self, context):
        from apache_beam.portability.api import beam_runner_api_pb2
        urn, typed_param = self.to_runner_api_parameter(context)
        payload = typed_param
        if callable(getattr(typed_param, "SerializeToString", None)):
            print "convert to string"
            payload=typed_param.SerializeToString()
        return beam_runner_api_pb2.FunctionSpec(
            urn=urn,
            payload=payload
            #payload=typed_param
            if typed_param is not None else None)

    def to_runner_api_parameter(self, context):
        assert isinstance(self, CustomKafkaInput), \
            "expected instance of CustomKafkaInput, but got %s" % self.__class__
        assert len(self.consumer_properties) > 0, "consumer properties not set"
        return ("custom:kafkaInput",
            #wrappers_pb2.BytesValue(value=json.dumps(properties)))
            json.dumps(self.consumer_properties).encode())

    @staticmethod
    @PTransform.register_urn("custom:kafkaInput", None)
    #@PTransform.register_urn("custom:kafkaInput", wrappers_pb2.BytesValue)
    def from_runner_api_parameter(spec_parameter, unused_context):
        print "spec: " + spec_parameter
        instance = CustomKafkaInput()
        instance.consumer_properties = json.loads(spec_parameter)
        return instance

    def set_kafka_consumer_property(self, key, value):
        self.consumer_properties[key] = value;
        return self

    def with_bootstrap_servers(self, bootstrap_servers):
        return self.set_kafka_consumer_property('bootstrap.servers', bootstrap_servers)

    def with_group_id(self, group_id):
        return self.set_kafka_consumer_property('group.id', group_id)


if __name__ == "__main__":
  runner = universal_local_runner.UniversalLocalRunner(
      runner_api_address="localhost:3000")

  options_string = sys.argv.extend(["--experiments=beam_fn_api"])
  pipeline_options = PipelineOptions(options_string)

  with beam.Pipeline(runner=runner, options=pipeline_options) as p:
    (p
        #| 'Create' >> beam.Create(['hello', 'world', 'world'])
        #| 'Read' >> ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
        | 'Kafka' >> CustomKafkaInput().with_bootstrap_servers('localhost:9092').with_group_id('beam-example-group')
        #| 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        #                .with_output_types(unicode))
        #| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        #| 'GroupAndSum' >> beam.CombinePerKey(sum)
        | beam.Map(lambda x: logging.info("Got %s", x) or (x, 1)))


