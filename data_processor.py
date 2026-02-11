"""
Data Resilience Module - Handles messy real-world data
"""
import pandas as pd
import numpy as np
from datetime import datetime
import re

class DataProcessor:
    def __init__(self):
        self.date_formats = ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y']
        
    def clean_work_orders(self, df):
        """Clean and normalize work orders data"""
        df_clean = df.copy()
        
        # Rename columns for consistency
        column_mapping = {
            'Deal name masked': 'deal_name',
            'Customer Name Code': 'customer_code',
            'Serial #': 'serial_number',
            'Nature of Work': 'work_type',
            'Sector': 'sector',
            'Execution Status': 'execution_status',
            'Amount in Rupees (Excl of GST) (Masked)': 'contract_value',
            'Billed Value in Rupees (Excl of GST.) (Masked)': 'billed_value',
            'Collected Amount in Rupees (Incl of GST.) (Masked)': 'collected_amount',
            'Amount Receivable (Masked)': 'receivable',
            'Date of PO/LOI': 'po_date',
            'Probable Start Date': 'start_date',
            'Probable End Date': 'end_date'
        }
        
        df_clean = df_clean.rename(columns=column_mapping)
        
        # Clean numeric columns
        numeric_cols = ['contract_value', 'billed_value', 'collected_amount', 'receivable']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
        
        # Normalize dates
        date_cols = ['po_date', 'start_date', 'end_date']
        for col in date_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(self._parse_date)
        
        # Normalize text fields
        text_cols = ['sector', 'work_type', 'execution_status']
        for col in text_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].str.strip().str.title()
        
        # Data quality flags
        df_clean['data_quality_score'] = self._calculate_quality_score(df_clean)
        
        return df_clean
    
    def clean_deals(self, df):
        """Clean and normalize deals funnel data"""
        df_clean = df.copy()
        
        # Remove header artifacts
        df_clean = df_clean[df_clean['Deal Status'] != 'Deal Status']
        
        # Clean deal values
        df_clean['Masked Deal value'] = pd.to_numeric(df_clean['Masked Deal value'], errors='coerce')
        
        # Map probability to numeric
        prob_map = {'High': 0.8, 'Medium': 0.5, 'Low': 0.2, '': 0.5}
        df_clean['probability_score'] = df_clean['Closure Probability'].map(prob_map).fillna(0.5)
        
        # Calculate weighted value
        df_clean['weighted_value'] = df_clean['Masked Deal value'] * df_clean['probability_score']
        
        # Normalize dates
        df_clean['Created Date'] = pd.to_datetime(df_clean['Created Date'], errors='coerce')
        df_clean['Tentative Close Date'] = pd.to_datetime(df_clean['Tentative Close Date'], errors='coerce')
        
        return df_clean
    
    def _parse_date(self, date_val):
        """Try multiple date formats"""
        if pd.isna(date_val) or date_val == '':
            return None
        
        for fmt in self.date_formats:
            try:
                return pd.to_datetime(date_val, format=fmt)
            except:
                continue
        return pd.to_datetime(date_val, errors='coerce')
    
    def _calculate_quality_score(self, df):
        """Calculate data completeness score per row"""
        critical_cols = ['contract_value', 'sector', 'execution_status']
        scores = []
        for _, row in df.iterrows():
            score = sum(1 for col in critical_cols if pd.notna(row.get(col)) and row.get(col) != 0)
            scores.append(score / len(critical_cols))
        return scores
    
    def get_revenue_metrics(self, wo_df):
        """Calculate key revenue metrics"""
        metrics = {
            'total_contract_value': wo_df['contract_value'].sum(),
            'total_billed': wo_df['billed_value'].sum(),
            'total_collected': wo_df['collected_amount'].sum(),
            'total_receivable': wo_df['receivable'].sum(),
            'billing_efficiency': (wo_df['billed_value'].sum() / wo_df['contract_value'].sum() * 100) 
                                  if wo_df['contract_value'].sum() > 0 else 0,
            'collection_rate': (wo_df['collected_amount'].sum() / wo_df['billed_value'].sum() * 100) 
                              if wo_df['billed_value'].sum() > 0 else 0
        }
        return metrics
    
    def get_pipeline_metrics(self, deals_df):
        """Calculate pipeline metrics"""
        open_deals = deals_df[deals_df['Deal Status'] == 'Open']
        
        metrics = {
            'total_open_pipeline': open_deals['Masked Deal value'].sum(),
            'weighted_pipeline': open_deals['weighted_value'].sum(),
            'open_deals_count': len(open_deals),
            'avg_deal_size': open_deals['Masked Deal value'].mean(),
            'win_rate': len(deals_df[deals_df['Deal Status'] == 'Won']) / len(deals_df) * 100
        }
        return metrics