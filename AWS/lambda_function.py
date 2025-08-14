import json
import logging
import os
import uuid
from datetime import datetime
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## AWS Lambda Function to interact with AWS Dynamodb amd AWS S3.

dynamo = boto3.resource("dynamodb")
table  = dynamo.Table(os.environ["JOBS_TABLE"])

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    for rec in event.get("Records", []):
        input_key = rec["s3"]["object"]["key"]

        filename = os.path.basename(input_key) 
        
        if "_" in filename:
            name_after_underscore = filename.split("_", 1)[1]
        else:
            name_after_underscore = filename

        job_id  = str(uuid.uuid4())
        now_iso = datetime.utcnow().isoformat()

        logger.info(
            "Processing S3 key %s → job_id %s (Name: %s)",
            input_key, job_id, name_after_underscore
        )

        table.put_item(Item={
            "JobId":        job_id,
            "InputKey":     input_key,
            "Name":         name_after_underscore,  
            "OutputFormat": "mp4",                  
            "Resolution":   "1280x720",             
            "VideoCodec":   "libx264",              
            "Status":       "PENDING",
            "CreatedAt":    now_iso
        })

    return {"status": "OK"}
