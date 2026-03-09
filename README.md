# End-to-End Data Pipeline

A complete data pipeline built with Prefect that ingests data from various formats (CSV, JSON, Excel, Parquet, XML), transforms it into a clean, normalized format, and outputs it in a friendly format for data analysis (CSV by default).

## Features

- **Multi-format ingestion**: Supports CSV, JSON, Excel, Parquet, and basic XML files
- **Data transformation**: Cleans data by removing duplicates, filling missing values, normalizing column names, and encoding categoricals (one-hot for >2 uniques, binary for ≤2)
- **Flexible output**: Outputs to Parquet, CSV, or JSON (CSV by default)
- **Orchestration**: Uses Prefect for workflow management
- **Configurable**: Easily configure output formats and directories via YAML config

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Running the Pipeline

```python
from src.pipeline import data_pipeline

# Run the pipeline on a directory
data_pipeline("path/to/input/directory")
```

Or run directly:

```bash
python src/pipeline.py

or 

python3 -m src.pipeline
```

### Configuration

Edit `config/config.yaml` to change output format and directory.

### Processing Logic

- Scans the input directory for supported files (CSV, JSON, Excel, Parquet, XML).
- For each file, checks if a processed version exists in `data/processed/`.
- If not processed, ingests, transforms, and outputs to `data/processed/`.
- Skips already processed files.

### Example
Input directory: `data/unprocessed/` with `{filename}.csv`.  
Output: `data/processed/{filename}_processed.csv` (cleaned CSV with hybrid categorical encoding). Skips if already exists.

### Supported Input Formats

- CSV
- JSON
- Excel (.xlsx, .xls)
- Parquet
- XML (simple structure)

### Output Formats

- CSV (default)
- Parquet
- JSON

## Project Structure

```
├── config/
│   └── config.yaml          # Configuration file
├── data/
│   ├── sample/              # Sample input data
│   └── processed/           # Output directory
├── src/
│   ├── pipeline.py          # Main pipeline flow
│   └── tasks/
│       ├── ingest.py        # Data ingestion tasks
│       ├── transform.py     # Data transformation tasks
│       └── output.py        # Data output tasks
├── tests/
│   └── test_pipeline.py     # Unit tests
├── requirements.txt         # Python dependencies
└── README.md
```

## Testing

Run tests with:

```bash
pytest
```

## Deployment

To deploy with Prefect Cloud or Server:

1. Set up Prefect (see Prefect docs)
2. Deploy the flow:

```python
from src.pipeline import data_pipeline
from prefect.deployments import Deployment

deployment = Deployment.build_from_flow(
    flow=data_pipeline,
    name="data-pipeline-deployment"
)
deployment.apply()
```