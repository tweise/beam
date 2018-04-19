import logging

import sys
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import universal_local_runner

from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.core import Windowing
from apache_beam import pvalue


class CustomKafkaInput(PTransform):
    """Custom transform primitive."""

    def expand(self, pbegin):
        assert isinstance(pbegin, pvalue.PBegin), (
                'Input to transform must be a PBegin but found %s' % pbegin)
        return pvalue.PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
        return Windowing(GlobalWindows())

    def infer_output_type(self, unused_input_type):
        return bytes

    def to_runner_api_parameter(self, context):
        assert isinstance(self, CustomKafkaInput), \
            "expected instance of CustomKafkaInput, but got %s" % self.__class__
        return ("custom:kafkaInput", None)

    @PTransform.register_urn("custom:kafkaInput", None)
    def from_runner_api_parameter(unused_parameter, unused_context):
        return CustomKafkaInput()


if __name__ == "__main__":
  runner = universal_local_runner.UniversalLocalRunner(
      runner_api_address="localhost:3000")

  options_string = sys.argv.extend(["--experiments=beam_fn_api"])
  pipeline_options = PipelineOptions(options_string)

  with beam.Pipeline(runner=runner, options=pipeline_options) as p:
    (p
        #| 'Create' >> beam.Create(['hello', 'world', 'world'])
        #| 'Read' >> ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
        | 'Kafka' >> CustomKafkaInput()
        #| 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        #                .with_output_types(unicode))
        #| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        #| 'GroupAndSum' >> beam.CombinePerKey(sum)
        | beam.Map(lambda x: logging.info("Got %s", x) or (x, 1)))


