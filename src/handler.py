import json
import os
from pymongo import MongoClient

db_uri = os.environ.get("MONGO_DB_URI", "localhost")
db_name = os.environ.get("MONGO_DB_NAME", "new_hire_test")

db = MongoClient(db_uri)[db_name]


def handle_csv_upload(event, context):
    response_body = {
        "numCreated": 0,
        "numUpdated": 0,
        "errors": [],
    }

    # YOUR LOGIC HERE

    response = {
        "statusCode": 200,
        "body": json.dumps(response_body)
    }

    return response
