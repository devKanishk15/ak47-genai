import requests
import boto3
import pandas as pd

# AWS S3 configuration
S3_BUCKET = "your-s3-bucket-name"
S3_FILE_KEY = "datasets/mandi_prices.csv"
s3 = boto3.client("s3")

# Government API for mandi prices (example API)
MANDI_API_URL = "https://api.example.gov/mandi-prices"

def fetch_mandi_prices():
    """Fetch real-time mandi prices from the government API."""
    response = requests.get(MANDI_API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to fetch mandi prices")

def update_mandi_prices():
    """Update the mandi prices dataset in S3."""
    try:
        # Fetch real-time mandi prices
        mandi_data = fetch_mandi_prices()
        df = pd.DataFrame(mandi_data)

        # Upload to S3 as CSV
        csv_buffer = df.to_csv(index=False)
        s3.put_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY, Body=csv_buffer)
        
        print("Mandi prices updated successfully.")
    except Exception as e:
        print(f"Error updating mandi prices: {e}")

# Run script
if __name__ == "__main__":
    update_mandi_prices()
