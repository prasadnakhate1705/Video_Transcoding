# Video_transcoding

## Overview

A Cloud-Native, distributed video transcoding pipeline designed for high scalability and low latency.  
It leverages Apache Spark running on Jetstream2 nodes in combination with AWS cloud services to split, process, and reassemble videos efficiently.  
By breaking videos into fixed-duration segments and transcoding them in parallel, the pipeline significantly reduces end-to-end processing time and supports adaptive bitrate streaming.


##  Features

- **Segment-based Parallel Transcoding**  
  - Splits videos into **2-minute chunks** using `FFmpeg`.
  - Each segment is processed independently and in parallel.

- **Apache Spark on YARN**  
  - Distributed segment transcoding across **Jetstream2 worker nodes**.
  - Dynamic executor allocation for optimal resource usage.

- **AWS Integration**
  - **S3** → Stores input videos, intermediate segments, and final HLS outputs.  
  - **Lambda** → Triggers transcoding jobs on upload events.  
  - **DynamoDB** → Maintains job metadata, enables fault-tolerant retries, and handles job locking.  
  - **CloudFront + HLS** → Delivers adaptive bitrate streaming to end users.

- **Fault Tolerance & Scalability**
  - Retry failed segments automatically.
  - Scale horizontally by adding more Spark executors.

---

## Architecture
```plaintext
        ┌────────────┐      ┌───────────────┐
        │   Upload   │      │ AWS Lambda    │
        │   to S3    │ ───▶ │ Trigger Job   │
        └────────────┘      └──────┬────────┘
                                    │
                           ┌────────▼─────────┐
                           │  Apache Spark    │
                           │ (Jetstream2 HPC) │
                           └──────┬───────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         ▼                        ▼                        ▼
   Segment 1                  Segment 2                 Segment N
  (FFmpeg)                    (FFmpeg)                   (FFmpeg)
         └──────────────┬───────────────┬─────────────────┘
                        ▼               ▼
                  Merge Segments    Generate HLS
                        │
                        ▼
                Upload to S3 (Final Output)
                        │
                        ▼
                 Stream via CloudFront
