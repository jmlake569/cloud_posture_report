import requests
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone

# Setup logging
logging.basicConfig(filename='api_audit.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REGION = "us-1"
BASE_URL = f"https://conformity.{REGION}.cloudone.trendmicro.com/v1"
C1_API_KEY = "YOUR-API-KEY-HERE"  # API key without 'ApiKey ' prefix
DAYS_BACK = 30  # Number of days back to fetch data

common_headers = {
    "Content-Type": "application/json",
    "Authorization": f"ApiKey {C1_API_KEY}",  # Prepend 'ApiKey ' to the authorization header
    "api-version": "v1"
}

def get_accounts():
    account_url = f"{BASE_URL}/accounts/"
    try:
        response = requests.get(account_url, headers=common_headers)
        logging.info(f"Request to {account_url} - Status Code: {response.status_code}")
        if response.status_code == 200:
            accounts_data = response.json()
            accounts = accounts_data.get('data', [])
            logging.info(f"Accounts fetched: {len(accounts)}")
            
            accounts_list = []
            for account in accounts:
                attributes = account.get('attributes', {})
                account_details = {
                    'id': account.get('id'),
                    'name': attributes.get('name'),
                    'environment': attributes.get('environment'),
                    'aws_account_id': attributes.get('awsaccount-id')
                }
                accounts_list.append(account_details)
                
            return accounts_list
        else:
            logging.error(f"Failed to fetch accounts. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def get_checks(account_id):
    checks_url = f"{BASE_URL}/checks"
    start_date = (datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')
    params = {'accountIds': account_id, 'startDate': start_date}  # Assuming API supports filtering by date
    try:
        response = requests.get(checks_url, headers=common_headers, params=params)
        logging.info(f"Request to {checks_url} with params {params} - Status Code: {response.status_code}")
        if response.status_code == 200:
            checks_data = response.json()
            checks_count = len(checks_data.get('data', []))
            logging.info(f"Checks fetched for account {account_id}: {checks_count}")
            return checks_data
        else:
            logging.error(f"Failed to fetch checks for account {account_id}. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def epoch_to_datetime(time_input):
    """
    Converts an epoch time or an ISO 8601 formatted string to a formatted date string.
    """
    if time_input is None:
        return None

    time_str = str(time_input)  # Convert input to string to handle both integers and strings

    try:
        # If the input is an epoch timestamp
        if time_str.isdigit():
            epoch = float(time_str)
            return datetime.fromtimestamp(epoch / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        # Directly parse the ISO 8601 string
        elif 'Z' in time_str:
            return datetime.fromisoformat(time_str.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return datetime.fromisoformat(time_str).astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        logging.error(f"Error converting time: {e}")
        return None

if __name__ == "__main__":
    accounts_info = get_accounts()
    failures = []
    successes = []
    if accounts_info:
        for account in accounts_info:
            account_id = account['id']
            checks_info = get_checks(account_id)
            if checks_info and 'data' in checks_info:
                for check in checks_info['data']:
                    attributes = check['attributes']
                    record = {
                        "account_id": account_id,
                        "account_name": account['name'],
                        "environment": account['environment'],
                        "aws_account_id": account['aws_account_id'],
                        "status": attributes["status"],
                        "message": attributes.get("message"),
                        "descriptorType": attributes.get("descriptorType"),
                        "resourceName": attributes.get("resourceName"),
                        "last-refresh-date": epoch_to_datetime(attributes.get("last-refresh-date")),
                        "last-modified-date": epoch_to_datetime(attributes.get("last-modified-date")),
                        "created-date": epoch_to_datetime(attributes.get("created-date")),
                        # Additional fields for failures
                        "failure-introduced-by": attributes.get("failure-introduced-by", ""),
                        "risk-level": attributes.get("risk-level", ""),
                        # Additional field for successes
                        "resolved-by": attributes.get("resolved-by", ""),
                    }
                    if attributes["status"] == "SUCCESS":
                        successes.append(record)
                    elif attributes["status"] == "FAILURE":
                        failures.append(record)

        # Convert lists to pandas DataFrames and export to Excel
        df_failures = pd.DataFrame(failures)
        df_successes = pd.DataFrame(successes)

        with pd.ExcelWriter('compliance_report.xlsx', engine='openpyxl') as writer:
            if not df_failures.empty:
                df_failures.to_excel(writer, sheet_name='Failures', index=False)
            if not df_successes.empty:
                df_successes.to_excel(writer, sheet_name='Successes', index=False)

        logging.info("Report generated successfully.")
    else:
        logging.warning("No accounts data found or failed to retrieve account details.")
