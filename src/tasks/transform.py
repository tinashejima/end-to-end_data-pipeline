import pandas as pd
from prefect import task
import logging

logger = logging.getLogger(__name__)

@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the data: clean, normalize, encode categoricals, etc.
    """
    try:
        # Drop duplicates
        df = df.drop_duplicates()
        
        # Fill missing values with forward fill, then backward fill
        df = df.fillna(method='ffill').fillna(method='bfill')
        
        # Normalize column names: lowercase, replace spaces with underscores
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        
        # Convert data types if possible
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col])
                except:
                    pass
        
        # Encode categorical columns
        categorical_cols = [col for col in df.select_dtypes(include=['object']).columns]
        for col in categorical_cols:
            unique_vals = df[col].unique()
            if len(unique_vals) > 2:
                # One-hot encode
                dummies = pd.get_dummies(df[col], prefix=col, drop_first=False).astype(int)
                df = pd.concat([df.drop(col, axis=1), dummies], axis=1)
            else:
                # Binary encode: sort unique values and assign 0/1
                sorted_vals = sorted(unique_vals)
                mapping = {sorted_vals[0]: 0, sorted_vals[1]: 1} if len(unique_vals) == 2 else {unique_vals[0]: 0}
                df[col] = df[col].map(mapping).astype(int)
        
        logger.info(f"Successfully transformed data, shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise