from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_restful import Api
from src.api import create_app, db


from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_restful import Api
from src.api import create_app, db


from views import (
    ViewUploadAndConvert,
)

app = create_app('conversor-batch')

app_context = app.app_context()
app_context.push()

cors = CORS(app, resources={r"/*": {"origins": "*"}})

api = Api(app)
api.add_resource(ViewUploadAndConvert, '/upload')
jwt = JWTManager(app)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)