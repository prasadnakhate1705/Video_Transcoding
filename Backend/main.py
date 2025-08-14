import os
import uuid
from flask import Flask, request, jsonify, render_template
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)


_raw_region = os.getenv("AWS_REGION", "")
AWS_REGION = _raw_region.split("#", 1)[0].strip() 

# S3
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
BUCKET = os.getenv("S3_BUCKET")

# DynamoDB resource & table
dynamo = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
jobs_table = dynamo.Table(os.getenv("JOBS_TABLE"))


# Upload Function.
@app.route("/upload", methods=["POST"])
def upload_video():

    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    filename = secure_filename(file.filename)
    job_id = str(uuid.uuid4())
    s3_key = f"videos/{job_id}_{filename}"

    try:
        s3.upload_fileobj(
            Fileobj=file.stream,
            Bucket=BUCKET,
            Key=s3_key,
            ExtraArgs={"ContentType": file.mimetype}
        )
    except (BotoCoreError, ClientError) as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "message": "Upload successful",
        "s3_key": s3_key
    }), 202


# List transcoded videos
@app.route("/videos")
def list_videos():
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="transcoded/")
    keys = [obj["Key"] for obj in resp.get("Contents", [])]
    return jsonify({"videos": keys})


# Stream Videos
@app.route("/stream")
def get_stream_url():
    key = request.args.get("key")
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": key},
        ExpiresIn=3600
    )
    return jsonify({"url": url})


if __name__ == "__main__":
    app.run(debug=True)

