from google.cloud import pubsub_v1
import json

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/conversor-403414/topics/video-conversion-requests'

def publish_to_pubsub(filename, target_format, current_user_id):
    message = json.dumps({
        'filename': filename,
        'target_format': target_format,
        'current_user_id': current_user_id,
    }).encode('utf-8')

    future = publisher.publish(topic_path, data=message)
    future.result()