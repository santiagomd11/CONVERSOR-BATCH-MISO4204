from google.cloud import pubsub_v1
import json
import os
from datetime import datetime
from moviepy.editor import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from google.cloud import storage
import tempfile

from src.models import (
    Task,
    FileExtensions,
    ConversionFile
)

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
GCP_BUCKET_NAME = 'conversor-bucket'

# Database engine setup
db_engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
Session = sessionmaker(bind=db_engine)

# Setup Google Cloud Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = 'projects/conversor-403414/subscriptions/video-conversion-requests-sub'

def convert_video(filename, target_format, current_user_id):
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCP_BUCKET_NAME)

    with tempfile.NamedTemporaryFile(suffix=filename, delete=False) as temp_file:
        blob = bucket.blob(f'videos/{filename}')
        blob.download_to_filename(temp_file.name)

        video = VideoFileClip(temp_file.name)
        converted_file_name = f'{filename.rsplit(".", 1)[0]}_converted.{target_format.lower()}'
        converted_file_path = tempfile.mktemp(suffix=converted_file_name)

        video.write_videofile(str(converted_file_path))

        converted_blob = bucket.blob(f'videos/{converted_file_name}')
        converted_blob.upload_from_filename(converted_file_path)

        timestamp = datetime.now()
        file_status = "processed"
        conversion_task = ConversionFile(file_name=converted_file_name, timestamp=timestamp, status=file_status)
        
        with Session() as session:
            session.add(conversion_task)
            session.commit()
        
            task = Task(
                original_file_name=filename,
                original_file_extension=FileExtensions(filename.split('.')[-1].lower()),
                converted_file_extension=FileExtensions(target_format.lower()),
                is_available=True,
                original_file_url=f'videos/{filename}',
                converted_file_url=f'videos/{converted_file_name}',
                user_id=current_user_id,
                conversion_file=conversion_task
            )
            session.add(task)
            session.commit()

        os.remove(temp_file.name)
        os.remove(converted_file_path)

def callback(message):
    print(f"Received message: {message}")
    data = json.loads(message.data.decode("utf-8"))
    convert_video(data['filename'], data['target_format'], data['current_user_id'])
    message.ack()

with subscriber:
    future = subscriber.subscribe(subscription_path, callback=callback)
    print(f'Listening for messages on {subscription_path}...')
    
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
