# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import functions_framework
import json
import logging
import random
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from flask import escape
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud import storage

import query_params


def download_blob(bucket_name, blob_name):
  '''Downloads a blob into memory.'''
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_name)
  return blob.download_as_string().decode('utf-8')

def get_bucket_blobs(bucket_name):
  '''Creates a dictionary of blob names and their content from a bucket.'''
  storage_client = storage.Client()
  blobs = storage_client.list_blobs(bucket_name)

  query_strings = {}
  for blob in blobs:
    query_strings[blob.name] = download_blob(bucket_name, blob.name)
  return query_strings

def run_query(query_string, test_id, project_id):
  bq_client = bigquery.Client(project=project_id)
  job_config = bigquery.QueryJobConfig(use_query_cache=False)
  job_config.labels = {'test_id': test_id}

  try:
    query_job = bq_client.query(query_string, job_config=job_config)
    query_job.result(timeout = float(120))
    logging.info(f'Query completed test_id {test_id} project_id {project_id}.')

  except GoogleAPICallError as e:
    logging.error('An error occurred: %s. Exiting.', e.message)
    raise e

def prepare_thread_pool_execution(bucket_name, n_queries):
  query_strings_dict = get_bucket_blobs(bucket_name)

  query_strings = []
  for _ in range(n_queries):
    file_name, query_string = random.choice([query_strings_dict.items()])
    # If a query string contains parameters, replace them with the respective
    # values coming from the execution of the functions defined in the
    # dictionary query_params.params_dict.
    if file_name in query_params.params_dict:
      replacement_defs = query_params.params_dict[file_name]
      replacements = {
          param_name: func() for (param_name, func) in replacement_defs.items()}
      query_strings.append(query_string.format(**replacements))
    else:
      query_strings.append(query_string)

  return query_strings

def run_queries(n_queries, bucket_name, concurrency, project_id, test_id,
cycles, wait_time):
  query_strings = prepare_thread_pool_execution(bucket_name, n_queries)

  for cycle in range(cycles):
    logging.info(f'Starting cycle {cycle} of test_id {test_id}.')
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
      executor.map(run_query, query_strings, [test_id] * n_queries, 
          [project_id] * n_queries)

    logging.info('Waiting for tasks to complete...')
    executor.shutdown()

    if cycle < cycles - 1: 
      time.sleep(wait_time)

def launch_queries(event, context):
  '''Background Cloud Function that launches queries.
  The Cloud Function gets parameters from Pub/Sub, reads sql files from
  a Cloud Storage bucket, and execute queries against BigQuery.
  Args:
    event:  The dictionary with data specific to this type of event. 
    context: Metadata of triggering event.
  Returns:
    None. The output is written to Cloud Logging.
  '''
  logging.info(
      f'This Function was triggered by messageId {context.event_id} published '
      f'at {context.timestamp} to {context.resource["name"]}.')

  if 'data' not in event:
    raise ValueError('data not present in Pub/Sub message. Exiting function.')

  data_dict = json.loads(base64.b64decode(event['data']).decode('utf-8'))
  logging.info(f'data received: {data_dict}')

  if 'project_id' not in data_dict:
    raise ValueError('project_id not present in event.')

  if 'bucket_name' not in data_dict:
    raise ValueError('bucket_name not present in event.')

  if 'test_id' not in data_dict:
    raise ValueError('test_id not present in event.')

  n_queries = data_dict.get('n_queries', 1)
  concurrency = data_dict.get('concurrency', 10)
  project_id = data_dict.get('project_id')
  bucket_name = data_dict.get('bucket_name')
  test_id = data_dict.get('test_id')
  cycles = data_dict.get('cycles', 1)
  wait_time = data_dict.get('wait_time', 0)

  logging.info(f'Run queries with properties n_queries={n_queries}, '
      f'concurrency={concurrency}, project_id={project_id}, cycles={cycles}, '
      f'wait_time={wait_time}, bucket_name={bucket_name}, test_id={test_id}')

  run_queries(n_queries=n_queries, bucket_name=bucket_name, 
      concurrency=concurrency, project_id=project_id, test_id=test_id, 
      cycles=cycles, wait_time=wait_time)