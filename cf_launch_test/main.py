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
import os
import requests
import time

from google.cloud import pubsub_v1


LAUNCH_QUERIES_TOPIC = (
    os.environ.get('LAUNCH_QUERIES_TOPIC', 'Specified env variable not set.'))
ADMIN_PROJECT = (
    os.environ.get('ADMIN_PROJECT', 'Specified env variable not set.'))

@functions_framework.http
def launch_test_http(request):
  '''HTTP-triggered Cloud Function that launches the test on BigQuery.
  The Cloud Function reads test parameters from the HTTP request and sends
  messages on a Pub/Sub Topic that subsequently triggers Cloud Functions.'''
  
  request_json = request.get_json(silent=True)
  if not request_json:
	  raise ValueError('HTTP request not present.')

  if 'project_id' not in request_json:
    raise ValueError('project_id not present in http request.')

  if 'bucket_name' not in request_json:
    raise ValueError('bucket_name not present in http request.')

  n_queries = request_json.get('n_queries', 1)
  n_functions = request_json.get('n_functions', 1)
  concurrency = request_json.get('concurrency', 50)
  cycles = request_json.get('cycles', 1)
  wait_time = request_json.get('wait_time', 40)
  project_id = request_json.get('project_id')
  bucket_name = request_json.get('bucket_name')
  current_time = time.strftime('%Y_%m_%d_%H_%M', time.gmtime())
  test_id = f'{current_time}_{project_id}'

  message_str = (f'{{"n_queries": {n_queries}, "n_functions": {n_functions}, '
      f'"bucket_name": "{bucket_name}", "concurrency": {concurrency}, '
      f'"cycles": {cycles}, "wait_time": {wait_time}, '
      f'"test_id": "{test_id}", "project_id": "{project_id}"}}')
  message = message_str.encode('utf-8')
  logging.info(f'Sending message to Pub/Sub: {message}')

  publisher = pubsub_v1.PublisherClient()
  for i in range(n_functions):
    topic_name = f'projects/{ADMIN_PROJECT}/topics/{LAUNCH_QUERIES_TOPIC}'
    publisher.publish(topic_name, message)
    logging.info(f'Function instance {i} launched.')

  return f'200 Launched test with id {test_id} on project {project_id}.'
