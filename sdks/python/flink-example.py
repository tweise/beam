import logging

import sys
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.portability import universal_local_runner

if __name__ == "__main__":
  runner = universal_local_runner.UniversalLocalRunner(
      runner_api_address="localhost:3000")

  options_string = sys.argv.extend(["--experiments=beam_fn_api"])
  pipeline_options = PipelineOptions(options_string)

  with beam.Pipeline(runner=runner, options=pipeline_options) as p:
    (p
        | 'Create' >> beam.Create(['hello', 'world', 'world'])
        #| 'Read' >> ReadFromText("gs://dataflow-samples/shakespeare/kinglear.txt")
        | 'Split' >> (beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
                        .with_output_types(unicode))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | beam.Map(lambda x: logging.info("Got %s", x) or (x, 1)))

