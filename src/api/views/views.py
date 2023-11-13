from flask import request
from flask import jsonify
from flask import current_app
from flask_restful import Resource
import hashlib
from flask import send_file
from pathlib import Path
from werkzeug.utils import secure_filename
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from src.celery.celery import convert_video_async

NFS_PATH = '/nfs/general'

class ConvertFile(Resource):
    def post(self):
        file = request.files['file']
        target_format = request.form['target_format']
        current_user_id = request.form['current_user_id']

        filename = secure_filename(file.filename)

        convert_video_async.delay(filename, target_format, current_user_id)

        return {'message': 'Conversion started asynchronously'}, 202