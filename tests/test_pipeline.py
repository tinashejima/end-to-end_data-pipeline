import pytest
import pandas as pd
import os
from src.tasks.ingest import detect_format, ingest_data
from src.tasks.transform import transform_data
from src.tasks.output import output_data

def test_detect_format():
    assert detect_format("test.csv") == "csv"
    assert detect_format("test.json") == "json"
    assert detect_format("test.xlsx") == "excel"
    assert detect_format("test.parquet") == "parquet"

def test_ingest_csv():
    # Assuming sample file exists
    df = ingest_data("data/sample/input.csv", "csv")
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] > 0

def test_transform_data():
    df = pd.DataFrame({"A": [1, 2, 2], "B": [None, 2, 3]})
    transformed = transform_data(df)
    assert transformed.shape[0] == 2  # duplicates removed
    assert not transformed.isnull().any().any()  # no nulls

def test_output_data(tmp_path):
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    output_path = tmp_path / "test.parquet"
    output_data(df, str(output_path), "parquet")
    assert output_path.exists()