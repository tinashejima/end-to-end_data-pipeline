import pandas as pd
from prefect import task
import logging

logger = logging.getLogger(__name__)

@task
def validate_schema(df: pd.DataFrame, expected_schema: dict = None) -> dict:
    """
    Validate data schema and quality.
    Returns validation report with issues.
    """
    report = {
        "is_valid": True,
        "issues": [],
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "dtypes": df.dtypes.to_dict(),
        "missing_values": df.isnull().sum().to_dict(),
    }
    
    # Check for completely empty rows
    empty_rows = df.isna().all(axis=1).sum()
    if empty_rows > 0:
        report["issues"].append(f"Found {empty_rows} completely empty rows")
        report["is_valid"] = False
    
    # Check for completely empty columns
    empty_cols = df.columns[df.isnull().all()].tolist()
    if empty_cols:
        report["issues"].append(f"Found {len(empty_cols)} completely empty columns: {empty_cols}")
        report["is_valid"] = False
    
    # Check if data is empty
    if len(df) == 0:
        report["issues"].append("DataFrame is empty")
        report["is_valid"] = False
    
    # Validate against expected schema if provided
    if expected_schema:
        missing_cols = set(expected_schema.keys()) - set(df.columns)
        if missing_cols:
            report["issues"].append(f"Missing expected columns: {missing_cols}")
            report["is_valid"] = False
        
        extra_cols = set(df.columns) - set(expected_schema.keys())
        if extra_cols:
            report["issues"].append(f"Unexpected columns found: {extra_cols}")
    
    if report["issues"]:
        logger.warning(f"Schema validation failed: {report['issues']}")
    else:
        logger.info("Schema validation passed")
    
    return report

@task
def detect_outliers(df: pd.DataFrame, numeric_cols: list = None) -> dict:
    """
    Detect outliers in numeric columns using IQR method.
    """
    if numeric_cols is None:
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    outliers_report = {"has_outliers": False, "columns": {}}
    
    for col in numeric_cols:
        if col not in df.columns:
            continue
        
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
        outlier_count = outlier_mask.sum()
        
        if outlier_count > 0:
            outliers_report["has_outliers"] = True
            outliers_report["columns"][col] = {
                "count": int(outlier_count),
                "percentage": float((outlier_count / len(df)) * 100),
                "bounds": {"lower": float(lower_bound), "upper": float(upper_bound)}
            }
    
    logger.info(f"Outlier detection complete: {outliers_report}")
    return outliers_report

@task
def data_quality_check(df: pd.DataFrame) -> dict:
    """
    Comprehensive data quality check.
    """
    check_result = {
        "passed": True,
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "checks": {}
    }
    
    # Missing values check
    missing_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
    check_result["checks"]["missing_percentage"] = float(missing_pct)
    if missing_pct > 50:  # Flag if >50% missing
        check_result["passed"] = False
        check_result["checks"]["missing_percentage_status"] = "FAILED"
    else:
        check_result["checks"]["missing_percentage_status"] = "PASSED"
    
    # Duplicate rows check
    duplicates = df.duplicated().sum()
    check_result["checks"]["duplicate_rows"] = int(duplicates)
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate rows")
    
    # Data type consistency
    check_result["checks"]["data_types"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
    
    logger.info(f"Data quality check result: {check_result}")
    return check_result
