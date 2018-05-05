from __future__ import print_function
import logging
import json

import sys
import apache_beam as beam
from apache_beam import combiners
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners.portability import portable_runner
from apache_beam.runners.direct.direct_runner import DirectRunner
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import TimestampedValue


ONE_HOUR_IN_SECONDS = 3600
MAX_TIMESTAMP = 0x7fffffffffffffff

class ExtractUserAndTimestampDoFn(beam.DoFn):
    """Extracts user and timestamp representing a Wikipedia edit."""

    def process(self, element):
        table_row = json.loads(element)
        if 'contributor_username' in table_row:
            user_name = table_row['contributor_username']
            timestamp = table_row['timestamp']
            yield TimestampedValue(user_name, timestamp)



class ComputeSessions(beam.PTransform):
    """Computes the number of edits in each user session.

    A session is defined as a string of edits where each is separated from the
    next by less than an hour.
    """
    def expand(self, pcoll):
        return (pcoll
                | 'ComputeSessionsWindow' >> beam.WindowInto(
                    Sessions(ONE_HOUR_IN_SECONDS))
                | combiners.Count.PerElement()
                )

class SessionsToStringsDoFn(beam.DoFn):
    """Adds the session information to be part of the key."""

    def process(self, element, window=beam.DoFn.WindowParam):
        #print(element)
        yield (element[0] + ' : ' + str(window), element[1])

if __name__ == "__main__":

  EDITS = [
        json.dumps({'timestamp': 0.0, 'contributor_username': 'user1'}),
        json.dumps({'timestamp': 0.001, 'contributor_username': 'user1'}),
        json.dumps({'timestamp': 0.002, 'contributor_username': 'user1'}),
        json.dumps({'timestamp': 0.0, 'contributor_username': 'user2'}),
        json.dumps({'timestamp': 0.001, 'contributor_username': 'user2'}),
        json.dumps({'timestamp': 3.601, 'contributor_username': 'user2'}),
        json.dumps({'timestamp': 3.602, 'contributor_username': 'user2'}),
        json.dumps(
            {'timestamp': 2 * 3600.0, 'contributor_username': 'user2'}),
        json.dumps(
            {'timestamp': 35 * 24 * 3.600, 'contributor_username': 'user3'})
  ]

  #EXPECTED = [
  #      'user1 : [0.0, 3600.002) : 3 : [0.0, 2592000.0)',
  #      'user2 : [0.0, 3603.602) : 4 : [0.0, 2592000.0)',
  #      'user2 : [7200.0, 10800.0) : 1 : [0.0, 2592000.0)',
  #      'user3 : [3024.0, 6624.0) : 1 : [0.0, 2592000.0)',
  #]

  #runner = DirectRunner()
  #options_string = sys.argv.extend([])

  runner = portable_runner.PortableRunner()
  options_string = sys.argv.extend(["--experiments=beam_fn_api", "--sdk_location=container", "--job_endpoint=localhost:8099"])

  pipeline_options = PipelineOptions(options_string)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(runner=runner, options=pipeline_options) as p:
    (p
       | 'Create' >> beam.Create(EDITS)
       | 'ExtractUserAndTimestamp' >> beam.ParDo(ExtractUserAndTimestampDoFn())
       #| beam.Filter(lambda x: (abs(hash(x)) <= MAX_TIMESTAMP * 0.1))
       | ComputeSessions()
       | 'SessionsToStrings' >> beam.ParDo(SessionsToStringsDoFn())
       | beam.Map(lambda x: logging.info("Got %s", x))
     )
