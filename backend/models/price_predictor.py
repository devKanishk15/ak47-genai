import boto3

# Initialize SageMaker runtime client
sagemaker = boto3.client("sagemaker-runtime")

def get_predicted_price(mcat_name, brand_category, state, district):
    """Calls SageMaker to get the predicted price range."""
    payload = {
        "mcat_name": mcat_name,
        "brand_category": brand_category,
        "state": state,
        "district": district
    }
    
    response = sagemaker.invoke_endpoint(
        EndpointName="price-prediction-model",
        ContentType="application/json",
        Body=json.dumps(payload)
    )

    result = json.loads(response["Body"].read().decode("utf-8"))
    return result.get("predicted_price_range", "N/A")
