import streamlit as st
import os
import base64
import requests
from dotenv import load_dotenv


load_dotenv()
FLASK_URL = os.getenv("FLASK_URL", "http://localhost:5000")

st.set_page_config(page_title="Transcodify", layout="wide")
PAGES = ["Home", "Upload", "Stream", "Results"]


if "page" not in st.session_state:
    st.session_state.page = "Home"


sel = st.sidebar.radio("Navigate to", PAGES, index=PAGES.index(st.session_state.page))
if sel != st.session_state.page:
    st.session_state.page = sel


def set_background(image_path: str):
    if not os.path.exists(image_path):
        return
    data = base64.b64encode(open(image_path, "rb").read()).decode()
    st.markdown(f"""
        <style>
          .stApp {{
            background: url("data:image/png;base64,{data}") center/cover no-repeat;
          }}
        </style>
    """, unsafe_allow_html=True)

set_background("hero.png")

# HOME PAGE
if st.session_state.page == "Home":
    st.markdown("""
      <div style="
        position:absolute;
        top:30%;
        left:50%;
        transform:translate(-50%,-30%);
        max-width:600px;
        background:rgba(0,0,0,0.6);
        padding:2rem;
        border-radius:8px;
        text-align:center;
      ">
        <h1 style="color:#fff; margin-bottom:1rem;">Welcome to Transcodify</h1>
        <p style="
          color:#ddd;
          font-size:1.1rem;
          line-height:1.5;
          margin-bottom:2rem;
        ">
          Transcodify is your go-to cloud-powered platform for parallel and distributed video
          transcoding. Upload, convert, and stream seamlessly—all in one place.
        </p>
        <!-- Use a button that changes the URL in-place -->
        <button 
          onclick="window.location.search='?page=Upload'"
          style="
            padding:0.75rem 1.5rem;
            background:#ff6f61;
            color:#fff;
            border:none;
            border-radius:4px;
            font-size:1rem;
            cursor:pointer;
          ">
          Get Started
        </button>
      </div>
    """, unsafe_allow_html=True)


# UPLOAD PAGE 
elif st.session_state.page == "Upload":
    st.title("📤 Upload Your Video")

    # single vs. parallel mode —
    mode = st.selectbox(
        "Mode",
        ["single", "parallel"]
    )

    # Existing transcoding parameters —
    output_format = st.selectbox(
        "Output format",
        ["mp4", "mov", "avi"]
    )
    resolution = st.selectbox(
        "Resolution",
        ["640x360", "1280x720", "1920x1080", "3840x2160"]
    )
    codec = st.selectbox(
        "Video codec",
        ["h264", "h265", "vp9", "av1"]
    )

    uploaded = st.file_uploader("", type=["mp4", "mov", "avi"])
    if uploaded:
        files = {
            "file": (
                uploaded.name,
                uploaded.getvalue(),
                uploaded.type
            )
        }
        data = {
            "mode":       mode,
            "format":     output_format,
            "resolution": resolution,
            "codec":      codec
        }

        try:
            resp = requests.post(f"{FLASK_URL}/upload", files=files, data=data)
            if resp.status_code == 202:
                st.success(f"Upload successful! S3 key: `{resp.json()['s3_key']}`")
            else:
                st.error(f" Upload failed: {resp.text}")
        except Exception as e:
            st.error(f"Error connecting to backend: {e}")



# STREAM PAGE
elif st.session_state.page == "Stream":
    st.title("Stream Your Video")
    try:
        resp = requests.get(f"{FLASK_URL}/videos")
        resp.raise_for_status()
        videos = resp.json().get("videos", [])
        if not videos:
            st.info("No transcoded videos found. Please upload one first.")
        else:
            choice = st.selectbox("Select a transcoded video", videos)
            if choice:
                r2 = requests.get(f"{FLASK_URL}/stream", params={"key": choice})
                r2.raise_for_status()
                st.video(r2.json()["url"])
    except Exception as e:
        st.error(f"⚠️ Error: {e}")

        
#  RESULTS PAGE 
elif st.session_state.page == "Results":
    st.title("Compare Single vs. Parallel Transcoding")

    import boto3
    import pandas as pd
    from botocore.exceptions import ClientError

    # Init DynamoDB
    ddb = boto3.resource(
        "dynamodb",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    table = ddb.Table(os.getenv("JOBS_TABLE"))

    # Fetch all records with Name, Mode, DurationSeconds
    try:
        resp = table.scan(
            ProjectionExpression="#n, #m, #d",
            ExpressionAttributeNames={
                "#n": "Name",
                "#m": "Mode",
                "#d": "DurationSeconds"
            }
        )
        items = resp.get("Items", [])
    except ClientError as e:
        st.error(f"Error fetching results: {e}")
        st.stop()

    if not items:
        st.info("No transcoding records found yet.")
        st.stop()

    df = pd.DataFrame(items)
    df["DurationSeconds"] = df["DurationSeconds"].astype(float).round(2)

    #  user picks a single video name
    names = sorted(df["Name"].unique())
    selected_name = st.selectbox("Select a video to compare", names)

    # Filter down to that Name
    df_sel = df[df["Name"] == selected_name]

    # show as bar chart
    pivot = df_sel.set_index("Mode")["DurationSeconds"]

    st.markdown(f"#### Durations for **{selected_name}**")
    st.bar_chart(pivot)
