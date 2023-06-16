import os
from google.cloud import pubsub_v1

TOPIC = 'launch_test'

publisher = pubsub_v1.PublisherClient()
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=os.getenv('GOOGLE_CLOUD_PROJECT'),
    topic=TOPIC,
)
future = publisher.publish(topic_name, b'My first message!', spam='eggs')
future.result()
