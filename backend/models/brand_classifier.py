import boto3

# Initialize Amazon Bedrock client
bedrock = boto3.client("bedrock-runtime")

def classify_brand(brand_name):
    """Classifies brand as Premium, Mid-range, or Generic using Amazon Bedrock."""
    prompt = f"Classify the brand '{brand_name}' as Premium, Mid-range, or Generic."
    
    response = bedrock.invoke_model(
        modelId="amazon.titan-text-express-v1",
        contentType="application/json",
        accept="application/json",
        body=json.dumps({"inputText": prompt})
    )

    result = json.loads(response["body"].read().decode("utf-8"))
    return result.get("completion", "Generic")  # Default to Generic if uncertain
