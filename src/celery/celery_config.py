from celery import Celery

celery_app = Celery(
    'conversor',
    broker='pyamqp://guest@localhost//',
    backend='rpc://',
)

celery_app.conf.update(
    result_expires=3600,
)