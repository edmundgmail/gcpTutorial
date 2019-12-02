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
from apache_beam.transforms.util import BatchElements

from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from googleapiclient import discovery

project_id = 'big-data-on-gcp1'
bucket_name='big-data-on-gcp-tweets-bucket'
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
        return

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
            "user_id": tweet["user"]["id"]
        }
        return [(1, processed_tweet)]

def init_api():
    global cmle_api

    # If it hasn't been instantiated yet: do it now
    if cmle_api is None:
        cmle_api = discovery.build('ml', 'v1',
                                   discoveryServiceUrl='https://storage.googleapis.com/cloud-ml/discovery/ml_v1_discovery.json',
                                   cache_discovery=True)

def estimate_cmle(instances):
    """
    Calls the tweet_sentiment_classifier API on CMLE to get predictions
    Args:
       instances: list of strings
    Returns:
        float: estimated values
    """

    # Init the CMLE calling api
    init_api()

    request_data = {'instances': instances}

    logging.info("making request to the ML api")

    # Call the model
    model_url = 'projects/YOUR_PROJECT/models/tweet_sentiment_classifier'
    response = cmle_api.projects().predict(body=request_data, name=model_url).execute()

    # Read out the scores
    values = [item["score"] for item in response['predictions']]

    return values

def estimate(messages):

    # Be able to cope with a single string as well
    if not isinstance(messages, list):
        messages = [messages]

    # Messages from pubsub are JSON strings
    instances = list(map(lambda message: json.loads(message), messages))

    # Estimate the sentiment of the 'text' of each tweet
    scores = estimate_cmle([instance["text"] for instance in instances])

    # Join them together
    for i, instance in enumerate(instances):
        instance['sentiment'] = scores[i]

    logging.info("first message in batch")
    logging.info(instances[0])

    return instances

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
            print(element)
            rows_to_insert.append(element)
            yield beam.window.TimestampedValue(element, time)

        #error = self.client.insert_rows(self.table, rows_to_insert)
        #assert error == []


class FormatDoFn(beam.DoFn):
  def process(self, element, window=beam.DoFn.WindowParam):
    ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
    window_start = window.start.to_utc_datetime().strftime(ts_format)
    window_end = window.end.to_utc_datetime().strftime(ts_format)
    return element[1]

def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--output_table', required=True,
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

  pipeline_args.extend([
      # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',
      # CHANGE 3/5: Your project ID is required in order to run your pipeline on
      # the Google Cloud Dataflow Service.
      '--project='+ project_id,
      # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=gs://'+bucket_name+'/dataflow',
      # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=gs://'+bucket_name+'/dataflow_temp',
      '--job_name=streaming-tweets-into-bq',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True

  pipeline = beam.Pipeline(options=pipeline_options)

  processed_tweets = (pipeline
       | 'Read PubSub Messages' >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
       | 'Parse Message' >> beam.ParDo(ParseMessage())
       | beam.WindowInto(window.FixedWindows(2 * 60, 0))
       | 'Group' >> beam.GroupByKey()
       | 'Format' >> beam.ParDo(FormatDoFn()))
       #| 'assign window key' >> beam.WindowInto(window.FixedWindows(10)))
       #| 'group by window key' >> beam.GroupByKey()
       #| 'Format' >> beam.ParDo(FormatDoFn()))
       #| 'predict sentiment' >> beam.FlatMap(lambda messages: estimate(messages)))
       # | 'group' >> beam.map(lambda messages: estimate(messages)))

  #processed_tweets | 'Write to BigQuery' >> beam.ParDo(WriteBatchesToBigQuery(known_args.output_dataset_id, known_args.output_table_id))

  #processed_tweets | 'Average' >> beam.parDo(DoAggregate) | 'Write average to Big Query' >> beam.Pardo(WriteBatchesToBigQuery(known_args.output_dataset_id, known_args.output_average_table_id))

  #(bigqueryschema,  bigqueryschema_mean) = getschema()

  #processed_tweets | 'store twitter posts' >> beam.io.WriteToBigQuery(
  #    table="mytable",
  #    dataset="mydataset",
  #    schema=bigqueryschema,
  #    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
  #    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
  #    project="big-data-on-gcp1"
  #)


  TABLE_SCHEMA = ('id:STRING, lang:STRING, retweeted_id:STRING, favorite_count:INT64, retweet_count:INT64, place:STRING, user_id:STRING, coordinates_latitude:FLOAT64, coordinates_longitude:FLOAT64')


  processed_tweets | 'Write' >> beam.io.WriteToBigQuery(
      table=known_args.output_table,
      schema=TABLE_SCHEMA,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)



  result = pipeline.run()
  result.wait_until_finish()
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
