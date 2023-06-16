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

bq mk \
--use_legacy_sql=false \
--view \
"WITH project_descriptions AS(
  SELECT 
    '${AUTOSCALING_PROJECT}' AS project_id, 
    'autoscaling' AS project_description, 
    '${AUTOSCALING_RESERVATION_NAME}' as reservation_name, 
    'autoscaling' AS pricing_model 
  UNION ALL
  SELECT 
    '${ONDEMAND_PROJECT}', 'on_demand', '', 'on_demand'
  UNION ALL
  SELECT 
    '${BIENGINE_PROJECT}', 'bi_engine', '', 'bi_engine'
),

reservation_info AS (
SELECT
  *,
  CONCAT(project_id, ':US.', reservation_name) AS reservation_id,
  LAG(change_timestamp)
    OVER(PARTITION BY project_id, reservation_name ORDER BY change_timestamp DESC) AS next_change
FROM
  \`region-us\`.INFORMATION_SCHEMA.RESERVATION_CHANGES
ORDER BY
  change_timestamp DESC
),

reservation_usage AS(
SELECT 
  *,
  DATETIME_DIFF(next_change, change_timestamp, MILLISECOND) AS diff_millis,
  DATETIME_DIFF(next_change, change_timestamp, MILLISECOND) * autoscale.current_slots AS reservation_slot_millis,
  DATETIME_DIFF(next_change, change_timestamp, MILLISECOND) * autoscale.current_slots / 1000 / 60 / 60 AS reservation_slot_hours
FROM reservation_info
),

job_stats AS(
  SELECT 
    project_id,
    pricing_model,
    job_id,
    creation_time,
    start_time,
    end_time,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS time_elapsed_ms,
    total_bytes_processed / 1000 / 1000 / 1000 AS total_gb_processed,
    total_bytes_processed / 1000 / 1000 / 1000 / 1000 AS total_tb_processed,
    total_slot_ms,
    total_slot_ms / 1000 / 60 / 60 AS total_slot_hr,
    total_bytes_billed / 1000 / 1000 / 1000 AS total_gb_billed,
    total_bytes_billed / 1000 / 1000 / 1000 / 1000 AS total_tb_billed,
    labels.value AS test_id,
    reservation_id
  FROM region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION, UNNEST(labels) AS labels
  JOIN project_descriptions
  USING(project_id)
  ORDER BY creation_time DESC
), 

test_details AS (
  SELECT 
    labels.value AS test_id,
    project_id,
    reservation_id,
    TIMESTAMP_DIFF(MAX(end_time), MIN(creation_time), SECOND) AS test_duration_sec,
    MIN(creation_time) AS test_start,
    MAX(end_time) AS test_end,
  FROM region-us.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION, UNNEST(labels) AS labels
  GROUP BY 1, 2, 3
),

test_stats AS (
  SELECT 
    test_id,
    job_stats.project_id,
    pricing_model,
    job_stats.reservation_id,
    test_duration_sec,
    test_details.test_start,
    test_details.test_end,
    AVG(time_elapsed_ms) AS avg_time_elapsed_ms,
    SUM(total_tb_processed) AS total_tb_processed,
    SUM(total_slot_hr) AS total_slot_hr,
    SUM(total_tb_billed) AS total_tb_billed,
  FROM job_stats
  JOIN test_details USING(test_id)
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)

# On-demand
SELECT
  test_stats.project_id,
  test_stats.pricing_model,
  test_stats.reservation_id,
  test_id,
  test_stats.test_start,
  test_stats.test_end,
  avg_time_elapsed_ms,
  total_tb_processed,
  total_slot_hr,
  total_tb_billed,
  NULL AS reservation_slot_hours,
FROM test_stats
JOIN test_details USING(test_id)
JOIN project_descriptions AS descriptions
  ON test_stats.project_id = descriptions.project_id
AND descriptions.pricing_model = 'on_demand'

UNION ALL

# Autoscaling Reservations
SELECT 
  tests.project_id, 
  pricing_model,
  tests.reservation_id,
  test_id, 
  test_start,
  test_end,
  avg_time_elapsed_ms,
  total_tb_processed,
  total_slot_hr,
  NULL AS total_tb_billed,
  SUM(reservation_slot_hours) AS reservation_slot_hours
FROM test_stats AS tests
JOIN reservation_usage AS reservations
ON tests.reservation_id = reservations.reservation_id
AND reservations.change_timestamp > tests.test_start 
AND reservations.change_timestamp < tests.test_end
AND tests.pricing_model = 'autoscaling'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION ALL

# BI Engine
SELECT 
  tests.project_id, 
  pricing_model,
  tests.reservation_id,
  test_id, 
  test_start,
  test_end,
  avg_time_elapsed_ms,
  total_tb_processed,
  total_slot_hr,
  total_tb_billed,
  NULL AS reservation_slot_hours
FROM test_stats AS tests
WHERE pricing_model = 'bi_engine'
ORDER BY test_id DESC" \
${BQ_DATASET}.${BQ_V_TEST_RESULTS}