import os
import json
import pandas as pd

from typing import Tuple, Union, List, Dict, Any
from datetime import datetime


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
    if len(df) > 0:
        df.to_csv(filepath, index=False, encoding='utf-8')
        return filepath
    return None


def read_from_csv(filename: str, folder: str = 'data', datetime_columns: list = ['created_datetime']) -> pd.DataFrame:
    """
    Read DataFrame from CSV file.

    Parameters:
    - filename: Name of the file
    - folder: Folder to read from (default: 'data')

    Returns:
    - DataFrame: Loaded data
    """
    if file_exists(filename, folder):
        filepath = os.path.join(folder, filename)
        df = pd.read_csv(filepath, encoding='utf-8')
        for datetime_column in datetime_columns:
            if datetime_column in df.columns:
                df[datetime_column] = pd.to_datetime(df[datetime_column])
        return df
    else:
        filepath = os.path.join(folder, filename)
        raise FileNotFoundError(f"File not found: {filepath}")


def save_to_json(data: Union[List, Dict], filename: str, folder: str = 'data') -> str:
    """
    Save list or dictionary to JSON file.

    Parameters:
    - data: List or dictionary to save
    - filename: Name of the file
    - folder: Folder to save in (default: 'data')

    Returns:
    - str: Path to saved file
    """
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)
    if len(data) > 0:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return filepath
    return None

def read_from_json(filename: str, folder: str = 'data') -> Union[List, Dict]:
    """
    Read list or dictionary from JSON file.

    Parameters:
    - filename: Name of the file
    - folder: Folder to read from (default: 'data')

    Returns:
    - Union[List, Dict]: Loaded data
    """
    if file_exists(filename, folder):
        filepath = os.path.join(folder, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        filepath = os.path.join(folder, filename)
        raise FileNotFoundError(f"File not found: {filepath}")


def get_date_range(start: str, end: str, format: str = '%Y-%m-%d') -> Tuple[datetime, datetime]:
    start_date = datetime.strptime(start, format)
    end_date = datetime.strptime(end, format)
    return start_date, end_date
