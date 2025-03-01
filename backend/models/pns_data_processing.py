import boto3
import pandas as pd
import numpy as np
import re
import time

# AWS Configuration
AWS_REGION = "us-east-1"
S3_BUCKET = "ak-47-solutions"
S3_PREFIX = "pns-data/"
bedrock_client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
transcribe_client = boto3.client("transcribe", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)

# Commodity Units Mapping
COMMODITY_UNITS = {
    "amul_pure_ghee": "KG",
    "basil_seeds": "KG",
    "dry_iron": "KG",
    "loose_tea": "LITRE",
    "ashwagandha": "KG",
    "mcb_box": "PIECE",
    "mild_steel_nail": "KG",
    "sodium_lauryl": "KG",
    "stainless_steel_coils": "PIECE",
    "termeric_finger": "KG",
    "white_phynl": "KG"
}

# Function to read CSV from S3
def read_csv_from_s3(bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"], header=0)  # Ensure headers are read correctly
    df.columns = df.columns.str.strip()  # Remove any leading/trailing spaces
    print("CSV Columns:", df.columns)  # Debugging output

    # Identify the correct column for processing
    if "Output" in df.columns:
        data_column = "Output"
    elif "Recording Link" in df.columns:
        data_column = "Recording Link"
    else:
        raise KeyError("CSV file must contain either 'Output' or 'Recording Link' column.")

    return df, data_column

# Function to extract numeric price values from text
def extract_price(text):
    prices = re.findall(r'\b\d{2,6}\b', text)  # Extract numbers with 2-6 digits
    prices = [int(p) for p in prices if 10 <= int(p) <= 100000]  # Filter reasonable price values
    return prices

# Function to use Amazon Bedrock for price extraction
def get_price_from_text(text):
    prompt = f"Extract all possible price values in INR from this text related to product pricing:\n\n{text}"
    response = bedrock_client.invoke_model(
        modelId="anthropic.claude-v2",  # Change to Claude 3 if available
        contentType="application/json",
        accept="application/json",
        body=f'{{"prompt": "{prompt}", "max_tokens": 100}}'
    )
    response_text = response["body"].read().decode("utf-8")
    return extract_price(response_text)

# Function to transcribe audio using Amazon Transcribe
def transcribe_audio(audio_url):
    job_name = f"transcribe-{int(time.time())}"
    transcribe_client.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={"MediaFileUri": audio_url},
        MediaFormat="mp3",
        LanguageCode="en-US",
        OutputBucketName=S3_BUCKET
    )

    # Wait for job to complete
    while True:
        status = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
        if status["TranscriptionJob"]["TranscriptionJobStatus"] in ["COMPLETED", "FAILED"]:
            break
        time.sleep(5)

    if status["TranscriptionJob"]["TranscriptionJobStatus"] == "COMPLETED":
        transcript_uri = status["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
        transcript_text = s3_client.get_object(Bucket=S3_BUCKET, Key=transcript_uri.split("/")[-1])["Body"].read().decode("utf-8")
        return transcript_text
    return ""

# Main processing for all commodities
commodity_prices = {}

for commodity in COMMODITY_UNITS.keys():
    csv_key = f"{S3_PREFIX}{commodity}.csv"
    
    try:
        df, data_column = read_csv_from_s3(S3_BUCKET, csv_key)  # Identify correct column
    except Exception as e:
        print(f"Error reading {commodity}.csv: {e}")
        continue  # Skip this commodity if there's an error

    df["Prices"] = [[] for _ in range(len(df))]  # Initialize Prices column with empty lists

    for index, row in df.iterrows():
        output = row[data_column]  # Use correct column dynamically

        if data_column == "Recording Link":
            transcript = transcribe_audio(output)  # Transcribe if audio link
            prices = get_price_from_text(transcript)
        else:
            prices = get_price_from_text(output)  # Extract prices from transcript
        
        df.at[index, "Prices"] = prices  # Store extracted prices

    # Aggregate prices for the commodity
    all_prices = [p for sublist in df["Prices"] for p in sublist]  # Flatten list
    if all_prices:
        commodity_prices[commodity] = {
            "Mean": np.mean(all_prices),
            "Median": np.median(all_prices),
            "5th Percentile": np.percentile(all_prices, 5),
            "95th Percentile": np.percentile(all_prices, 95)
        }

# Create final DataFrame
final_df = pd.DataFrame.from_dict(commodity_prices, orient="index").reset_index()
final_df.columns = ["Commodity", "Mean", "Median", "5th Percentile", "95th Percentile"]

# Save to CSV and upload to S3
final_csv_path = "/tmp/commodity_prices.csv"
final_df.to_csv(final_csv_path, index=False)
s3_client.upload_file(final_csv_path, S3_BUCKET, "commodity_prices_summary.csv")
print(f"Final CSV saved at: {final_csv_path} and uploaded to S3 as commodity_prices_summary.csv")
