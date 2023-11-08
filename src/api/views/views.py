from flask import request
from flask import jsonify
from flask import current_app
from flask_restful import Resource
import hashlib
from flask import send_file
from pathlib import Path
from datetime import datetime
from moviepy.editor import *
from werkzeug.utils import secure_filename
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import (
    db,
    User,
    UserSchema,
    Task,
    TaskSchema,
    FileExtensions,
    ConversionFile
)

user_schema = UserSchema()
task_schema = TaskSchema()

NFS_PATH = '/nfs/general'

from celery import Celery
import multiprocessing


celery_app = Celery(
    'conversor-batch',
    broker='pyamqp://guest@localhost//',
    backend='rpc://',
)

celery_app.conf.update(
    result_expires=3600,
)

@celery_app.task
def convert_video_async(filename, target_format, current_user_id):
    video = VideoFileClip(filename)
    original_extension = filename.split('.')[1]
    converted_file_name = filename.split('.')[0] + '_converted' + '.' + target_format.lower()

    converted_file_path = os.path.join(NFS_PATH, converted_file_name)

    video.write_videofile(str(converted_file_path))
    
    timestamp = datetime.now()
    file_status = "processed"
    conversion_task = ConversionFile(file_name=converted_file_name, timestamp=timestamp, status=file_status)
    db.session.add(conversion_task)
    db.session.commit()
    
    task = Task(original_file_name=filename, original_file_extension=FileExtensions(original_extension.lower()),
                converted_file_extension=FileExtensions(target_format.lower()), is_available=True,
                original_file_url=filename, converted_file_url=converted_file_name,
                user_id=current_user_id, conversion_file=conversion_task)
    db.session.add(task)
    db.session.commit()


class ViewUploadAndConvert(Resource):
    @jwt_required()
    def get(self):
        auth_token = request.headers.get('Authorization')
        current_user_id = get_jwt_identity() 
        if not auth_token:
            return {'message': 'Token de autenticación inválido'}, 401 
        
        file = request.files['file']
        target_format = request.form['target_format']
        
        if target_format.lower() not in [e.value for e in FileExtensions]:
            return {'message': 'Formato de destino no admitido'}, 400 
        
        
        filename = secure_filename(file.filename)
        file.save(filename)

        p = multiprocessing.Process(target=convert_video_async, args=(filename, target_format, current_user_id))
        p.start()

        return {'message': 'La conversión se ha iniciado de manera asíncrona.'}, 200