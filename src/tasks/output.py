import pandas as pd
import os
from prefect import task
import logging

logger = logging.getLogger(__name__)

@task
def output_data(df: pd.DataFrame, output_path: str, output_format: str = 'parquet'):
    """
    Output the transformed data to the specified format.
    """
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if output_format == 'parquet':
            df.to_parquet(output_path, index=False)
        elif output_format == 'csv':
            df.to_csv(output_path, index=False)
        elif output_format == 'json':
            df.to_json(output_path, orient='records')
        else:
            raise ValueError(f"Unsupported output format {output_format}")
        
        logger.info(f"Successfully output data to {output_path}")
    except Exception as e:
        logger.error(f"Error outputting data to {output_path}: {e}")
        raise