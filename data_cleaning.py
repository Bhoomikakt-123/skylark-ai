import pandas as pd
import numpy as np


def clean_columns(df):
    """Standardize column names."""
    if df.empty:
        return df
    
    df.columns = df.columns.astype(str).str.strip().str.lower()
    return df


def convert_currency(df, column_name):
    """Convert currency strings to numeric values."""
    if column_name in df.columns:
        # Handle multiple currency formats and separators
        df[column_name] = (
            df[column_name]
            .astype(str)
            .str.replace(r'[₹$,]', '', regex=True)  # Remove currency symbols
            .str.replace(r',', '', regex=False)       # Remove thousand separators
            .str.strip()
        )
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0)
    return df


def convert_dates(df, column_name):
    """Convert date strings to datetime."""
    if column_name in df.columns:
        df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    return df


def clean_status_values(df, column_name):
    """Standardize status values."""
    if column_name in df.columns:
        df[column_name] = df[column_name].astype(str).str.strip().str.title()
    return df


def clean_work_orders(df):
    """Complete cleaning pipeline for work orders."""
    df = clean_columns(df)
    
    # Currency columns
    currency_cols = [
        "billed value in rupees (incl of gst.) (masked)",
        "collected amount in rupees (incl of gst.) (masked)", 
        "amount receivable (masked)"
    ]
    for col in currency_cols:
        df = convert_currency(df, col)
    
    # Date columns
    date_cols = ["collection date", "actual billing month", "created date"]
    for col in date_cols:
        df = convert_dates(df, col)
    
    # Fill numeric NaNs with 0, keep others as is
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    return df


def clean_deals(df):
    """Complete cleaning pipeline for deals."""
    df = clean_columns(df)
    
    # Currency
    df = convert_currency(df, "masked deal value")
    
    # Dates
    date_cols = ["close date (a)", "created date", "expected close date"]
    for col in date_cols:
        df = convert_dates(df, col)
    
    # Standardize status
    df = clean_status_values(df, "deal status")
    
    # Fill numeric NaNs
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    return df