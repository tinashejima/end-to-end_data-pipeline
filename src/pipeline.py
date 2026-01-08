from prefect import flow, task
import os
import yaml
from src.tasks.ingest import detect_format, ingest_data
from src.tasks.transform import transform_data
from src.tasks.output import output_data
import logging

# Load config
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logging.basicConfig(level=config['logging']['level'])

@flow
def data_pipeline(file_path: str, output_path: str = None):
    """
    Complete data pipeline: ingest, transform, output.
    
    Args:
        file_path: Path to the input data file
        output_path: Path to the output file. If None, auto-generate based on config.
    """
    if output_path is None:
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        output_format = config['pipeline']['output_format']
        output_dir = config['pipeline']['output_dir']
        output_path = os.path.join(output_dir, f"{base_name}_processed.{output_format}")
    
    format_type = detect_format(file_path)
    df = ingest_data(file_path, format_type)
    transformed_df = transform_data(df)
    output_data(transformed_df, output_path, config['pipeline']['output_format'])

if __name__ == "__main__":
    # Example run
    data_pipeline("data/unprocessed/input.csv")