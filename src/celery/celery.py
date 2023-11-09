from celery import Celery
from datetime import datetime
from moviepy.editor import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

db_engine = create_engine('postgresql://admin:miso4204@db:5432/miso4204db')
Session = sessionmaker(bind=db_engine)

@celery_app.task
def convert_video_async(filename, target_format, current_user_id):
    session = Session()
    video = VideoFileClip(filename)
    original_extension = filename.split('.')[1]
    converted_file_name = filename.split('.')[0] + '_converted' + '.' + target_format.lower()

    converted_file_path = os.path.join(NFS_PATH, converted_file_name)

    video.write_videofile(str(converted_file_path))
    
    timestamp = datetime.now()
    file_status = "processed"
    conversion_task = ConversionFile(file_name=converted_file_name, timestamp=timestamp, status=file_status)
    session.add(conversion_task)
    session.commit()
    
    task = Task(original_file_name=filename, original_file_extension=FileExtensions(original_extension.lower()),
                converted_file_extension=FileExtensions(target_format.lower()), is_available=True,
                original_file_url=filename, converted_file_url=converted_file_name,
                user_id=current_user_id, conversion_file=conversion_task)
    session.add(task)
    session.commit()
