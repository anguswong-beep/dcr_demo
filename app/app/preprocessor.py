from app.s3_loader import load_sales_data
import pandas as pd

from datetime import datetime

def preproc(df: pd.DataFrame) -> pd.DataFrame:
    """
    preprocess dates(str) -> dt
    """
    # convert to dt, coerce for error handling
    df['sale_date'] = pd.to_datetime(df['sale_date'],errors="coerce") 

    # pad zeros 
    df['sale_start_time'] = df['sale_start_time'].astype(str).str.zfill(6) 
    df['sale_end_time'] = df['sale_end_time'].astype(str).str.zfill(6) 
    
    #parse str -> datetime.time
    df['sale_start_time'] = pd.to_datetime(df['sale_start_time'], format="%H%M%S").dt.time
    df['sale_end_time'] = pd.to_datetime(df['sale_end_time'], format="%H%M%S").dt.time

    #combine date + time, could add pd.notnull
    df['sale_start_time'] = df.apply(
        lambda row: datetime.combine(row['sale_date'].date(), row['sale_start_time']),
        axis = 1
        )
    df['sale_end_time'] = df.apply(
        lambda row: datetime.combine(row['sale_date'].date(), row['sale_end_time']),
        axis = 1)
    df['sale_type'] = df['sale_type'].mask(df['refund_flag']==9, 'Refunded')

    df['sale_type'] = df['sale_type'].mask(df['refund_flag'] =='VOIDED','Voided')
    
    df['product_description']

    mask = df['product_description'].astype(str).str.contains(r'[%+]', regex=True)
    df.loc[mask,'product_description'] = (df.loc[mask,'product_description'].astype(str).str.replace(r'[%+]',' ',regex=True))
    
    return df

