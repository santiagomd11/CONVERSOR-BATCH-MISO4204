import json
import os
from datetime import datetime
from moviepy.editor import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from google.cloud import storage
import tempfile
from src.models import Task, FileExtensions, ConversionFile
import base64
import logging

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
GCP_BUCKET_NAME = os.getenv('GCP_BUCKET_NAME')

logging.basicConfig(level=logging.INFO)

# Database engine setup
db_engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
Session = sessionmaker(bind=db_engine)

def convert_video(filename, target_format, current_user_id):
    try: 
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCP_BUCKET_NAME)

        with tempfile.NamedTemporaryFile(suffix=filename, delete=False) as temp_file:
            blob = bucket.blob(f'videos/{filename}')
            blob.download_to_filename(temp_file.name)

            video = VideoFileClip(temp_file.name)
            converted_file_name = f'{filename.rsplit(".", 1)[0]}_converted.{target_format.lower()}'
            converted_file_path = tempfile.mktemp(suffix=converted_file_name)

            video.write_videofile(converted_file_path)

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
            return True
    
    except Exception as e:
        logging.error(f'Error during video conversion: {e}')
        return False

def handle_pubsub_message(pubsub_message):
    logging.info(f"Received message: {pubsub_message}")
    message_data_base64 = pubsub_message.get('data')

    if message_data_base64:
        message_data_bytes = base64.b64decode(message_data_base64)
        message_data = message_data_bytes.decode('utf-8')
        data = json.loads(message_data)

        message_id = pubsub_message.get('messageId')
        logging.info(f"Processing message with ID: {message_id}")

        return convert_video(data['filename'], data['target_format'], data['current_user_id'])
    else:
        logging.warning('No data found in the message')
        return False
