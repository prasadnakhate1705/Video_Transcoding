# ECCDynamicVideoTranscoder

## 📌 Overview
**ECCDynamicVideoTranscoder** is a **full-stack, cloud-native, distributed video transcoding system**.  
It combines an intuitive **web interface** with a **scalable backend pipeline** powered by **Apache Spark** on **Jetstream2 HPC** and AWS cloud services.  
Users can upload videos, choose between **single-node** or **multi-node** processing, and compare performance metrics — all in one platform.

---

## Key Features

### 🎥 Frontend
- User-friendly **video upload** form.
- Processing mode selection: **Single Node** or **Multi Node**.
- Real-time job status tracking (`Pending → Processing → Completed`).
- Performance comparison charts for both processing modes.
- Integrated HLS video player for streaming results via CloudFront.

### ⚙ Backend Processing
- **AWS S3** for storing uploaded videos, intermediate segments, and final outputs.
- **AWS Lambda** triggers job creation in **MongoDB** with initial status `Pending`.
- **Jetstream2 HPC** runs **PySpark** jobs to transcode video segments using FFmpeg.
- **Single Node Mode**: Entire video transcoded on one Jetstream node.
- **Multi Node Mode**: Video split into chunks and processed in parallel across multiple nodes.
- **MongoDB** stores job metadata, status updates, and performance metrics.
- **AWS CloudFront + HLS** delivers adaptive bitrate streaming.

---

## Architecture

```plaintext
          ┌────────────┐
          │   Frontend │
          │  (React)   │
          └─────┬──────┘
                │ Upload Video
                ▼
        ┌───────────────┐
        │ S3 Input Bucket│
        └─────┬─────────┘
              │ S3 Event
              ▼
       ┌──────────────┐
       │ AWS Lambda   │
       │ Create Mongo │
       │ Record (Pending)
       └─────┬────────┘
             │
             ▼
    ┌────────────────────┐
    │ Jetstream2 Cluster │
    │   PySpark + FFmpeg │
    └─────┬──────────────┘
          │
  ┌───────┴────────┐
  │ Single Node     │
  │ Multi Node      │
  └───────┬────────┘
          │
          ▼
  ┌───────────────────────┐
  │ S3 Output Bucket       │
  │ (HLS Segments + M3U8)  │
  └───────┬───────────────┘
          │
          ▼
    ┌─────────────┐
    │ CloudFront  │
    │ Streaming   │
    └─────┬───────┘
          │
          ▼
     Frontend Player


