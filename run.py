import argparse
import yaml
import pandas as pd
import numpy as np
import logging
import json
import sys
import time
import csv
from pathlib import Path


def write_error_metrics(output_path, version, message):
    error_output = {
        "version": version if version else "v1",
        "status": "error",
        "error_message": message
    }
    with open(output_path, "w") as f:
        json.dump(error_output, f, indent=2)

    print(json.dumps(error_output, indent=2))
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        filename=args.log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Job started")
    start_time = time.time()

    version = config.get("version", "v1")

    try:
        # -------------------------
        # 1️⃣ Load + Validate Config
        # -------------------------
        if not Path(args.config).exists():
            raise FileNotFoundError("Config file not found")

        with open(args.config) as f:
            config = yaml.safe_load(f)

        required_fields = ["seed", "window", "version"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing config field: {field}")

        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(f"Config validated | seed={seed}, window={window}, version={version}")

        # -------------------------
        # 2️⃣ Load + Validate Dataset
        # -------------------------
        if not Path(args.input).exists():
            raise FileNotFoundError("Input CSV not found")

        df = pd.read_csv(
            args.input,
            sep=",",
            engine="python",
            quoting=csv.QUOTE_NONE
        )

        if df.empty:
            raise ValueError("Input CSV is empty")

        # Clean column names
        df.columns = df.columns.str.replace('"', '').str.strip().str.lower()
        df = df.replace('"', '', regex=True)

        if "close" not in df.columns:
            raise ValueError("Missing required 'close' column in dataset.")

        logging.info(f"Rows loaded: {len(df)}")

        # Ensure numeric close
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        if df["close"].isna().all():
            raise ValueError("'close' column contains no valid numeric values.")

        # -------------------------
        # 3️⃣ Rolling Mean
        # -------------------------
        logging.info("Computing rolling mean")
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        # -------------------------
        # 4️⃣ Signal Generation
        # -------------------------
        logging.info("Generating signals")
        df["signal"] = np.where(
            df["close"] > df["rolling_mean"], 1, 0
        )

        # Exclude first window-1 rows (NaNs)
        valid_signals = df["signal"][window - 1:]

        signal_rate = float(valid_signals.mean())
        rows_processed = len(df)

        latency_ms = int((time.time() - start_time) * 1000)

        # -------------------------
        # 5️⃣ Metrics Output
        # -------------------------
        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=2)

        logging.info(f"Metrics computed: {metrics}")
        logging.info("Job completed successfully")

        print(json.dumps(metrics, indent=2))
        sys.exit(0)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        write_error_metrics(args.output, version, str(e))
        print(json.dumps({
            "version": version,
            "status": "error",
            "error_message": str(e)
        }, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
