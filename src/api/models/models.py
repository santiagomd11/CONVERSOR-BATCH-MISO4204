from flask_sqlalchemy import SQLAlchemy
from marshmallow import fields
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from enum import Enum
from datetime import datetime

db = SQLAlchemy()

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user = db.Column(db.String(128))
    email = db.Column(db.String(150))
    password = db.Column(db.String(150))
    tasks = db.relationship('Task', backref='user', lazy=True)
    
    
class FileExtensions(str, Enum):
    MP4 = 'mp4'
    WEBM = 'webm'
    AVI = 'avi'
    MPEG = 'mpeg'
    WMV = 'wmw'    

class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    original_file_name = db.Column(db.String(2000))
    original_file_extension = db.Column(db.Enum(FileExtensions))
    converted_file_extension  = db.Column(db.Enum(FileExtensions))
    is_available =  db.Column(db.Boolean)
    original_file_url = db.Column(db.String(2000))
    converted_file_url = db.Column(db.String(2000))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    conversion_file = db.relationship('ConversionFile', uselist=False, backref='task')
    
class ConversionFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String(200))
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(20))
    task_id = db.Column(db.Integer, db.ForeignKey('task.id'), unique=True)