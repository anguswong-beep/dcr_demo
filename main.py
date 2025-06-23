## app/main.py
from app.eventbridge_publisher import publish_event
from app.s3_loader import load_sales_data
from app.preprocessor import preproc
from app.eventbridge_publisher import publish_event

app = FastAPI()

df =load_sales_data('sales_export.csv')
df = preproc(df)
a =1

@app.get("/departments/profitability")
def top_departments(limit: int = 2):
    tmp = df.copy()  
    tmp["profit"] = tmp["total_sales"] - tmp["cost"]

    table = tmp.groupby("sub_department").agg({
        "sale_qty": "sum",
        "qty": "sum",
        "profit": "sum"
    })

    table = table.sort_values("profit", ascending=False).head(limit)
    return table.reset_index().to_dict(orient="records")

@app.get("departments/category")
def top_category(limit: int = 3):
    tmp = df.copy()  
    tmp["profit"] = tmp["total_sales"] - tmp["cost"]

    table = tmp.groupby("category").agg({
        "sale_qty": "sum",
        "qty": "sum",
        "profit": "sum",

    })
    table = table.sort_values("profit", ascending=False).head(limit)
    return table.reset_index().to_dict(orient="records")


@app.get("/refund-cost")
def refund_cost(limit: int = 3):
    tmp = df.copy()  
    table = tmp.groupby("sale_type").agg({
        "qty": "sum",
        "profit": "sum",
        "cost":'sum'

    })
    table = table.sort_values("qty", ascending=False).head(limit)
    return table.reset_index().to_dict(orient="records")

@app.get("/sale_types")
def sale_types(limit: int = 3):
    tmp = df.copy()  
    table = tmp.groupby("category").agg({
        "sale_qty": "sum",
        "qty": "sum",
        "profit": "sum",

    })
    table = table.sort_values("profit", ascending=False).head(limit)
    return table.reset_index().to_dict(orient="records")

@app.get("/customer-favorites")
def customer_fav(limit: int = 5):
    tmp = df.copy()  
    counts = tmp.groupby(['customer_name', 'product_description']).size().reset_index(name='count')
    favorite_items = counts.sort_values('count', ascending=False).drop_duplicates('customer_name')
    favorite_items = favorite_items.sort_values('customer_name')

    return favorite_items

a = 1


