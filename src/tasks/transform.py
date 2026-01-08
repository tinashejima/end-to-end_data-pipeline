import pandas as pd
from prefect import task
import logging
from sklearn.preprocessing import LabelEncoder

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
        
        # Encode categorical columns (object type that are not datetime) to numerical
        le = LabelEncoder()
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = le.fit_transform(df[col].astype(str))
        
        logger.info(f"Successfully transformed data, shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise