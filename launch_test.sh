#!/bin/bash
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


source /etc/profile

for PROJECT in ${ONDEMAND_PROJECT} ${AUTOSCALING_PROJECT} ${BIENGINE_PROJECT}
do
  JSON_STRING='{
    "n_queries": 10,
    "n_functions": 1,
    "project_id": "'"${PROJECT}"'",
    "bucket_name": "'"${QUERIES_BUCKET}"'",
    "concurrency": 20,
    "cycles": 1,
    "wait_time": 40
  }'
  echo "Launching test with details: ${JSON_STRING}"
  
  curl https://${REGION}-${ADMIN_PROJECT}.cloudfunctions.net/launch_test_http  \
    -X POST -H 'Content-Type: application/json' \
    -H "Authorization: bearer $(gcloud auth print-identity-token)" \
    -d "${JSON_STRING}"
done