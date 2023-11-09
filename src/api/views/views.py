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
        # Get the uploaded file and additional data
        file = request.files['file']
        target_format = request.form['target_format']
        current_user_id = request.form['current_user_id']
        
        # Save the file
        filename = secure_filename(file.filename)
        file_path = os.path.join(NFS_PATH, filename)
        file.save(file_path)
        
        # Start the conversion process using multiprocessing
        convert_video_async.delay(file_path, target_format, current_user_id)

        # Respond that the conversion has been started
        return {'message': 'Conversion started asynchronously'}, 202