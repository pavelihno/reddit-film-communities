import os
import pandas as pd


def save_to_csv(df: pd.DataFrame, filename: str, folder: str = 'data') -> str:
    """
    Save DataFrame to CSV file.

    Parameters:
    - df: DataFrame to save
    - filename: Name of the file
    - folder: Folder to save in (default: 'data')

    Returns:
    - str: Path to saved file
    """
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, index=False, encoding='utf-8')
    return filepath


def file_exists(filename: str, folder: str = 'data') -> bool:
    """
    Check if a file exists.

    Parameters:
    - filename: Name of the file
    - folder: Folder to check in (default: 'data')

    Returns:
    - bool: True if file exists, False otherwise
    """
    filepath = os.path.join(folder, filename)
    return os.path.exists(filepath)


def read_from_csv(filename: str, folder: str = 'data', datetime_columns: list = ['created_datetime']) -> pd.DataFrame:
    """
    Read DataFrame from CSV file.

    Parameters:
    - filename: Name of the file
    - folder: Folder to read from (default: 'data')

    Returns:
    - DataFrame: Loaded data
    """
    filepath = os.path.join(folder, filename)
    if os.path.exists(filepath):
        df = pd.read_csv(filepath, encoding='utf-8')
        for datetime_column in datetime_columns:
            if datetime_column in df.columns:
                df[datetime_column] = pd.to_datetime(df[datetime_column])
        return df
    else:
        raise FileNotFoundError(f"File not found: {filepath}")
