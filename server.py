from flask import Flask, jsonify, Response
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
from selenium.webdriver.firefox.options import Options as FirefoxOptions

import pytz
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
from dotenv import load_dotenv
from webdriver_manager.firefox import GeckoDriverManager


# Load environment variables from .env file
load_dotenv()

# Access variables
url = os.getenv('EXTRACTION_URL')
FOLDER_ID = os.getenv('GDRIVE_FOLDER_ID')
SCOPES = [os.getenv('GDRIVE_SCOPES')]

# Configure Service Account Info with variables
SERVICE_ACCOUNT_INFO = {
    "type": os.getenv('GDRIVE_TYPE'),
    "project_id": os.getenv('GDRIVE_PROJECT_ID'),
    "private_key_id": os.getenv('GDRIVE_PRIVATE_KEY_ID'),
    "private_key": os.getenv('GDRIVE_PRIVATE_KEY').replace("\\n", "\n"),
    "client_email": os.getenv('GDRIVE_CLIENT_EMAIL'),
    "client_id": os.getenv('GDRIVE_CLIENT_ID'),
    "auth_uri": os.getenv('GDRIVE_AUTH_URI'),
    "token_uri": os.getenv('GDRIVE_TOKEN_URI'),
    "auth_provider_x509_cert_url": os.getenv('GDRIVE_AUTH_PROVIDER_CERT_URL'),
    "client_x509_cert_url": os.getenv('GDRIVE_CLIENT_CERT_URL')
}

app = Flask(__name__)

# Function to upload a file to Google Drive
def upload_to_drive(file_name, new_df):
    credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
    service = build('drive', 'v3', credentials=credentials)
    # Check if the file already exists in Google Drive by searching with the file name
    results = service.files().list(q=f"'{FOLDER_ID}' in parents and name='{file_name}'", spaces='drive', fields='files(id, name)').execute()
    items = results.get('files', [])

    if len(items) > 0:
        # File exists, get its ID and download the file as .xlsx
        file_id = items[0]['id']

        # Download the file as an Excel file
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO(request.execute())
        
        # Load the existing data from the file into a DataFrame
        existing_df = pd.read_excel(fh)

        # Append new data to the existing data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # Save the updated data locally
        combined_df.to_excel(file_name, index=False)

        # Delete the existing file on Drive
        service.files().delete(fileId=file_id).execute()

        # Upload the updated file as a new file
        file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
        media = MediaFileUpload(file_name, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    else:
        # If the file doesn't exist, create a new one and upload it
        new_df.to_excel(file_name, index=False)
        file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
        media = MediaFileUpload(file_name, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()



# Function to round time to the nearest 15-minute interval
def round_to_15_minutes(dt):
    minutes = (dt.minute // 15) * 15
    return dt.replace(minute=minutes, second=0, microsecond=0)

# Function to extract data
def extract_data():
    while True:
        # driver = webdriver.Chrome()  # Initialize the Chrome driver here
        options = FirefoxOptions()
        options.add_argument("--headless")
        driver = webdriver.Firefox(options=options)
        try:
            # Open the website
            driver.get(url)
            attempts = 0
            india_total_demand = None
            while attempts < 50:
                try:
                    india_total_demand = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "india_total_demand"))
                    )
                    demand_last_updated = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "india_total_demand_last_updated"))
                    )
                    if india_total_demand.text.strip() and demand_last_updated.text.strip():
                        break
                except:
                    print("Element not found or not populated yet.")
                time.sleep(5)
                attempts += 1

            if india_total_demand and india_total_demand.text.strip() and demand_last_updated and demand_last_updated.text.strip():
                demand_text = india_total_demand.text.strip()
                last_updated_text = demand_last_updated.text.strip()
                if last_updated_text == "just now":
                    last_updated_text = "0 minutes ago"
                minutes_ago = int(''.join(filter(str.isdigit, last_updated_text)))
                current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
                adjusted_time = current_time - timedelta(minutes=minutes_ago)
                rounded_time = round_to_15_minutes(adjusted_time)
                end_time = rounded_time + timedelta(minutes=15)
                data = {
                    "Start Time": [rounded_time.strftime('%Y-%m-%d %H:%M:%S')],
                    "End Time": [end_time.strftime('%Y-%m-%d %H:%M:%S')],
                    "Demand": [demand_text]
                }
                df = pd.DataFrame(data)
                file_name = f"demand_data_{adjusted_time.strftime('%Y-%m-%d')}.xlsx"
                # if os.path.exists(file_name):
                #     with pd.ExcelWriter(file_name, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                #         start_row = writer.sheets['Sheet1'].max_row
                #         df.to_excel(writer, sheet_name='Sheet1', index=False, header=False, startrow=start_row)
                # else:
                #     df.to_excel(file_name, index=False)

                        # Upload the extracted data to Google Drive
                upload_to_drive(file_name, df)
                print("Data extraction completed successfully.")
            else:
                print("Data extraction failed.")
        
        except Exception as e:
            print(f"Error: {e}")
        
        finally:
            driver.quit()
        
        # Wait for 300 seconds before running again
        time.sleep(300)

# Flask route to trigger the extraction
@app.route('/extract', methods=['GET'])

def extract_route():
    def generate():
        while True:
            extract_data()
            yield f"data: Running extraction at {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    return Response(generate(), mimetype='text/event-stream')

# Run the Flask app
if __name__ == '__main__':
    app.run(debug=True)