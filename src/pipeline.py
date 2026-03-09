from prefect import flow, task
import os
import yaml
import time
from src.tasks.ingest import detect_format, ingest_data, list_files
from src.tasks.transform import transform_data
from src.tasks.output import output_data
from src.tasks.validation import validate_schema, detect_outliers, data_quality_check
from src.tasks.notifications import send_error_notification, send_success_notification, log_pipeline_metrics
import logging

# Load config
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=config['logging']['level'],
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    Complete end-to-end data pipeline: ingest, validate, transform, output for all unprocessed files.
    
    Args:
        input_dir: Directory containing input data files.
    """
    files = list_files(input_dir)
    logger.info(f"Found {len(files)} files to process in {input_dir}")
    
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    
    for file_path in files:
        try:
            if should_process(file_path):
                logger.info(f"Processing file: {file_path}")
                start_time = time.time()
                
                # Ingest
                format_type = detect_format(file_path)
                df = ingest_data(file_path, format_type)
                input_rows = len(df)
                
                # Validate
                schema_report = validate_schema(df)
                if not schema_report["is_valid"]:
                    logger.error(f"Schema validation failed for {file_path}: {schema_report['issues']}")
                    send_error_notification(
                        str(schema_report['issues']),
                        file_path,
                        "schema_validation"
                    )
                    failed_count += 1
                    continue
                
                quality_check = data_quality_check(df)
                if not quality_check["passed"]:
                    logger.warning(f"Data quality check failed for {file_path}")
                    send_error_notification(
                        str(quality_check),
                        file_path,
                        "data_quality"
                    )
                
                outliers = detect_outliers(df)
                if outliers["has_outliers"]:
                    logger.warning(f"Outliers detected in {file_path}: {outliers['columns']}")
                
                # Transform
                transformed_df = transform_data(df)
                output_rows = len(transformed_df)
                
                # Output
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                output_path = os.path.join(
                    config['pipeline']['output_dir'],
                    f"{base_name}_processed.{config['pipeline']['output_format']}"
                )
                output_data(transformed_df, output_path, config['pipeline']['output_format'])
                
                # Metrics
                transformation_time = time.time() - start_time
                metrics = log_pipeline_metrics(
                    file_path,
                    input_rows,
                    output_rows,
                    transformation_time,
                    output_path
                )
                
                # Send success notification
                send_success_notification(file_path, output_rows, output_path)
                
                processed_count += 1
                logger.info(f"Successfully processed {file_path} in {transformation_time:.2f}s")
            else:
                print(f"ALREADY PROCESSED: Skipping {file_path} (output exists in {config['pipeline']['output_dir']})")
                logger.info(f"File {file_path} already processed, skipping.")
                skipped_count += 1
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}", exc_info=True)
            send_error_notification(str(e), file_path, "processing_error")
            failed_count += 1
    
    summary = {
        "total_files": len(files),
        "processed": processed_count,
        "skipped": skipped_count,
        "failed": failed_count
    }
    logger.info(f"Pipeline Summary: {summary}")
    return summary

if __name__ == "__main__":
    # Example run
    data_pipeline("data/unprocessed")