#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A streaming word-counting workflow.
"""

from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import bigquery
import json
import time
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """
    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (pcoll
                # Assigns window info to each Pub/Sub message based on its
                # publish timestamp.
                | 'Window into Fixed Intervals' >> beam.WindowInto(window.FixedWindows(self.window_size))
                | 'Add Dummy Key' >> beam.Map(lambda elem: (None, elem))
                | 'Groupby' >> beam.GroupByKey()
                | 'Abandon Dummy Key' >> beam.MapTuple(lambda _, val: val))

class DoAggregate(beam.DoFn):
    def __init__(self):
        pass

    def process(self, batch):
        ret = {
            "total_favorite_count": sum(e['favorite_count'] for e in batch),
            "total_retweet_count": sum(e['retweet_count'] for e in batch)
        }
        return ret

class ParseMessage(beam.DoFn):
    def __init__(self):
        pass

    def process(self, element):
        tweet = (json.loads(element))
        processed_tweet = {
            "id": tweet["id"],
            "lang": tweet["lang"],
            "retweeted_id": tweet["retweeted_status"]["id"] if "retweeted_status" in tweet else None,
            "favorite_count": tweet["favorite_count"] if "favorite_count" in tweet else 0,
            "retweet_count": tweet["retweet_count"] if "retweet_count" in tweet else 0,
            "coordinates_latitude": tweet["coordinates"]["coordinates"][0] if tweet["coordinates"] else 0,
            "coordinates_longitude": tweet["coordinates"]["coordinates"][0] if tweet["coordinates"] else 0,
            "place": tweet["place"]["country_code"] if tweet["place"] else None,
            "user_id": tweet["user"]["id"],
            "created_at": time.mktime(time.strptime(tweet["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
        }
        return processed_tweet


class WriteBatchesToBigQuery(beam.DoFn):
    def __init__(self, dataset_id, table_id):
        # For this sample, the table must already exist and have a defined schema
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        self.table = client.get_table(table_ref)

    def process(self, batch, window=beam.DoFn.WindowParam):
        """Write one batch per file to a Google Cloud Storage bucket. """
        rows_to_insert = []
        for element in batch:
            rows_to_insert.append(element)

        #error = self.client.insert_rows(self.table, rows_to_insert)
        #assert error == []

def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_dataset_id', required=True,
      help=('Output BigQuery table Name'))

  parser.add_argument(
      '--output_table_id', required=True,
      help=('Output BigQuery table Name'))

  parser.add_argument(
      '--output_average_table_id', required=True,
      help=('Output BigQuery table Name'))


  parser.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  parser.add_argument(
        '--window_size',
        type=float,
        default=1.0,
        help='Output file\'s window size in number of minutes.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True

  pipeline = beam.Pipeline(options=pipeline_options)

  processed_tweets = (pipeline
       | 'Read PubSub Messages' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
       | 'decode' >> beam.Map( lambda x: x.decode('utf-8'))
        | 'Parse Message' >> beam.ParDo(ParseMessage())
       | 'Window into' >> GroupWindowsIntoBatches(known_args.window_size))

  processed_tweets | 'Write to BigQuery' >> beam.ParDo(WriteBatchesToBigQuery(known_args.output_dataset_id, known_args.output_table_id))

  #processed_tweets | 'Average' >> beam.parDo(DoAggregate) | 'Write average to Big Query' >> beam.Pardo(WriteBatchesToBigQuery(known_args.output_dataset_id, known_args.output_average_table_id))

  result = pipeline.run()
  result.wait_until_finish()
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
