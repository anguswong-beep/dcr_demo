import boto3
import pandas as pd
from io import StringIO
def load_sales_data(filename:str,bucket="dcrposdemo"):
    s3 = boto3.client("s3", region_name="us-east-2")

    print(f'loading {filename}')
    try:
        res = s3.get_object(Bucket=bucket,Key=filename)
        content = res['Body'].read().decode('ISO-8859-1')
        df = pd.read_csv(StringIO(content))
        return df
    except Exception as e:
        print(f'error: {e}')
        raise



