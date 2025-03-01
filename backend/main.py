from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from models.price_predictor import get_predicted_price
from models.brand_classifier import classify_brand
from services.brand_manager import get_brand_category

app = FastAPI()

# Input model for API
class PriceRequest(BaseModel):
    mcat_name: str
    brand_name: str
    state: str
    district: str

# API to predict price range
@app.post("/predict_price")
def predict_price(data: PriceRequest):
    try:
        # Check if brand exists in predefined list
        brand_category = get_brand_category(data.mcat_name, data.brand_name)
        
        # If brand not found, classify it dynamically
        if brand_category is None:
            brand_category = classify_brand(data.brand_name)
        
        # Call SageMaker for price prediction
        price_range = get_predicted_price(data.mcat_name, brand_category, data.state, data.district)

        return {
            "mcat_name": data.mcat_name,
            "brand_name": data.brand_name,
            "brand_category": brand_category,
            "price_range": price_range
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
