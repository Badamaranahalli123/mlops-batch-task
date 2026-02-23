# MLOps Engineering Technical Assessment

## Overview

This project implements a minimal MLOps-style batch pipeline that:

- Loads configuration from YAML
- Validates dataset input
- Computes rolling mean on `close`
- Generates binary trading signals
- Outputs structured JSON metrics
- Logs detailed execution steps
- Runs locally and inside Docker

The job is deterministic (seed-controlled), observable (logs + metrics), and deployment-ready (Dockerized).

---

## Project Structure

mlops-task/
│
├── run.py
├── config.yaml
├── data.csv
├── requirements.txt
├── Dockerfile
├── README.md
├── metrics.json   (generated after run)
└── run.log        (generated after run)

---

## Local Setup

### Install Dependencies

pip install -r requirements.txt

### Run Locally

python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log

---

## Docker Usage

### Build Docker Image

docker build -t mlops-task .

### Run Container

docker run --rm mlops-task

The container:
- Executes the batch job
- Prints metrics JSON to stdout
- Writes metrics.json and run.log
- Exits with code 0 on success

---

## Example metrics.json Output

{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 141,
  "seed": 42,
  "status": "success"
}

---

## Key Features

- Deterministic execution using seed from config
- Robust config and dataset validation
- Structured JSON metrics (machine-readable)
- Detailed logging for observability
- No hardcoded paths
- Dockerized for reproducible deployment

---

## Dependencies

- pandas
- numpy
- pyyaml