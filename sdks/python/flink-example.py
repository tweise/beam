import apache_beam as beam
from apache_beam import pvalue
from apache_beam.portability import common_urns
from apache_beam.runners.portability import universal_local_runner
from apache_beam.transforms.window import GlobalWindows

runner = universal_local_runner.UniversalLocalRunner(
    runner_api_address="localhost:3000")

class Impulse(beam.PTransform):
    """Primitive impulse primitive."""

    def expand(self, pbegin):
        assert isinstance(pbegin, pvalue.PBegin), (
                'Input to Impulse transform must be a PBegin but found %s' % pbegin)
        return pvalue.PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
        return beam.Windowing(GlobalWindows())

    def infer_output_type(self, unused_input_type):
        return bytes

    def to_runner_api_parameter(self, context):
      assert isinstance(self, Impulse), \
          "expected instance of ParDo, but got %s" % self.__class__
      return (common_urns.IMPULSE_TRANSFORM, None)

    @beam.PTransform.register_urn(common_urns.IMPULSE_TRANSFORM, None)
    def from_runner_api_parameter(unused_parameter, unused_context):
      return Impulse()

with beam.Pipeline(runner=runner) as p:
    p | Impulse() | beam.ParDo(lambda: [1, 2, 3]) | beam.Map(lambda x: x*x)

