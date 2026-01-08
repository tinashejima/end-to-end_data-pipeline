import pandas as pd
import os
from prefect import task
import logging

logger = logging.getLogger(__name__)

@task
def detect_format(file_path: str) -> str:
    """
    Detect the format of the input file based on extension.
    """
    ext = os.path.splitext(file_path)[1].lower()
    format_map = {
        '.csv': 'csv',
        '.json': 'json',
        '.xlsx': 'excel',
        '.xls': 'excel',
        '.parquet': 'parquet',
        '.xml': 'xml'
    }
    if ext in format_map:
        return format_map[ext]
    else:
        raise ValueError(f"Unsupported file format for {file_path}")

@task
def ingest_data(file_path: str, format_type: str) -> pd.DataFrame:
    """
    Ingest data from file into a pandas DataFrame.
    """
    try:
        if format_type == 'csv':
            df = pd.read_csv(file_path)
        elif format_type == 'json':
            df = pd.read_json(file_path)
        elif format_type == 'excel':
            df = pd.read_excel(file_path)
        elif format_type == 'parquet':
            df = pd.read_parquet(file_path)
        elif format_type == 'xml':
            # For XML, assume it's a simple structure, convert to dict then df
            import xml.etree.ElementTree as ET
            tree = ET.parse(file_path)
            root = tree.getroot()
            data = []
            for child in root:
                data.append({elem.tag: elem.text for elem in child})
            df = pd.DataFrame(data)
        else:
            raise ValueError(f"Unsupported format {format_type}")
        logger.info(f"Successfully ingested data from {file_path}, shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error ingesting data from {file_path}: {e}")
        raise