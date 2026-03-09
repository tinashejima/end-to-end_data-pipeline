import pytest
import pandas as pd
import os
from src.tasks.ingest import detect_format, ingest_data
from src.tasks.transform import transform_data
from src.tasks.output import output_data
from src.tasks.validation import validate_schema, detect_outliers, data_quality_check

def test_detect_format():
    assert detect_format("test.csv") == "csv"
    assert detect_format("test.json") == "json"
    assert detect_format("test.xlsx") == "excel"
    assert detect_format("test.parquet") == "parquet"

def test_validate_schema():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    report = validate_schema(df)
    assert report["is_valid"] == True
    assert report["row_count"] == 3
    assert report["column_count"] == 2

def test_validate_schema_empty():
    df = pd.DataFrame()
    report = validate_schema(df)
    assert report["is_valid"] == False
    assert "empty" in report["issues"][0].lower()

def test_detect_outliers():
    df = pd.DataFrame({"A": [1, 2, 3, 100], "B": [4, 5, 6, 7]})
    result = detect_outliers(df)
    assert result["has_outliers"] == True
    assert "A" in result["columns"]

def test_data_quality_check():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    result = data_quality_check(df)
    assert result["total_rows"] == 3
    assert result["total_columns"] == 2

def test_transform_data():
    df = pd.DataFrame({"A": [1, 2, 2], "B": [None, 2, 3], "Name": ["John", "Jane", "John"]})
    transformed = transform_data(df)
    assert transformed.shape[0] == 2  # duplicates removed
    assert "Name" not in transformed.columns  # categorical encoded
    assert not transformed.isnull().any().any()  # no nulls

def test_output_data(tmp_path):
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    
    # Test CSV output
    output_path_csv = tmp_path / "test.csv"
    output_data(df, str(output_path_csv), "csv")
    assert output_path_csv.exists()
    
    # Test Parquet output
    output_path_parquet = tmp_path / "test.parquet"
    output_data(df, str(output_path_parquet), "parquet")
    assert output_path_parquet.exists()
    
    # Test JSON output
    output_path_json = tmp_path / "test.json"
    output_data(df, str(output_path_json), "json")
    assert output_path_json.exists()