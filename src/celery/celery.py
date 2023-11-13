from celery import Celery
from datetime import datetime
from moviepy.editor import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from google.cloud import storage
import tempfile

from src.api.models import (
    Task,
    FileExtensions,
    ConversionFile
)

celery_app = Celery(
    'conversor-batch',
    broker='pyamqp://guest:guest@rabbitmq//', 
    backend='rpc://rabbitmq',
)

celery_app.conf.update(
    result_expires=3600,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

NFS_PATH = '/nfs/general'

POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
POSTGRES_DB = os.environ.get('POSTGRES_DB')
POSTGRES_USER = os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD')

db_engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
Session = sessionmaker(bind=db_engine)

GCP_BUCKET_NAME = 'conversor-bucket'

@celery_app.task
def convert_video_async(filename, target_format, current_user_id):
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
    
        task = Task(original_file_name=filename, original_file_extension=FileExtensions(filename.split('.')[-1].lower()),
                    converted_file_extension=FileExtensions(target_format.lower()), is_available=True,
                    original_file_url=f'videos/{filename}', converted_file_url=f'videos/{converted_file_name}',
                    user_id=current_user_id, conversion_file=conversion_task)
        session.add(task)
        session.commit()

    os.remove(temp_file.name)
    os.remove(converted_file_path)