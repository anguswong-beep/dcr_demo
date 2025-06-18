## app/main.py
from fastapi import FastAPI
from app.s3_loader import load_sales_data
from app.processor import summarize_sales
from app.eventbridge_publisher import publish_event

app = FastAPI()

@app.get("/analyze-sales")
def analyze_sales():
    df = load_sales_data("data.csv")
    #summary = summarize_sales(df)
    print(df.head(5))    
    #publish_event(summary)
    print(123)
    return {'rows': len(df),"columns":df.columns.tolist()}


