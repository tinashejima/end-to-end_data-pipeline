from prefect import flow, task
import os
import yaml
from src.tasks.ingest import detect_format, ingest_data, list_files
from src.tasks.transform import transform_data
from src.tasks.output import output_data
import logging

# Load config
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logging.basicConfig(level=config['logging']['level'])

@task
def should_process(file_path: str) -> bool:
    """
    Check if the file has already been processed.
    """
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_format = config['pipeline']['output_format']
    output_path = os.path.join(config['pipeline']['output_dir'], f"{base_name}_processed.{output_format}")
    return not os.path.exists(output_path)

@flow
def data_pipeline(input_dir: str):
    """
    Complete data pipeline: ingest, transform, output for all unprocessed files in input_dir.
    
    Args:
        input_dir: Directory containing input data files.
    """
    files = list_files(input_dir)
    for file_path in files:
        if should_process(file_path):
            format_type = detect_format(file_path)
            df = ingest_data(file_path, format_type)
            transformed_df = transform_data(df)
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            output_path = os.path.join(config['pipeline']['output_dir'], f"{base_name}_processed.{config['pipeline']['output_format']}")
            output_data(transformed_df, output_path, config['pipeline']['output_format'])
        else:
            print(f"ALREADY PROCESSED: Skipping {file_path} (output exists in {config['pipeline']['output_dir']})")
            logging.info(f"File {file_path} already processed, skipping.")

if __name__ == "__main__":
    # Example run
    data_pipeline("data/unprocessed")