import apache_beam as beam
from apache_beam.runners.portability import universal_local_runner

runner = universal_local_runner.UniversalLocalRunner(
    runner_api_address="localhost:3000")
with beam.Pipeline(runner=runner) as p:
    p | beam.Create(range(10)) | beam.Map(lambda x: x*x)

