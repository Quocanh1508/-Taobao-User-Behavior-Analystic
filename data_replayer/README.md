# Data Replayer

This script simulates streaming line-by-line of historical dataset into batched JSON files partitioned by ingestion date and hour.
This mock can be later adapted to stream into Pub/Sub or actual GCS buckets using google-cloud APIs.

## Installation

Ensure Python 3.10+ is installed.

```bash
# Set up a virtual environment (if not using Poetry)
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux/MacOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Make sure your raw zip file is located in `../data/raw`. Settings like `batch_size_mb` and `source_data` are accessible in `config.yaml`.

## Usage

```bash
# Example command to stream 1,000,000 rows quickly (10,000x faster than real time) into a local mock bucket
python replayer.py --speed-factor 10000 --target-bucket gs_mock_bucket --max-rows 1000000

# Example command to start data simulation from a specific start date
python replayer.py --speed-factor 1000 --target-bucket gs_mock_bucket --start-date 2017-11-25
```
