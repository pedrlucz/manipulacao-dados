from dotenv import load_dotenv
from flask import Flask
from flask_sqlalchemy import SQLAlchemy 
import os

load_dotenv()

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)

    load_dotenv()
    
    app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    # inicialize o db com o app
    db.init_app(app)

    # inicializa as tabelas
    with app.app_context():
        db.create_all()

    return app

db_config = {
                'host': os.getenv("DB_HOST"),
                    'user': os.getenv("DB_USER"),
                        'password': os.getenv("DB_PASSWORD"),
                            'database': os.getenv("DB_NAME")
                                                                }