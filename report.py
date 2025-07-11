import requests
import logging
import pandas as pd
import argparse
import json
from datetime import datetime, timedelta, timezone
import urllib.parse

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Generate Cloud Posture compliance report',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python report.py --token "your_token_here" --timeframe 7 --all
  python report.py --token "your_token_here" --timeframe 30 --f --aws
  python report.py --token "your_token_here" --timeframe 7 --s --azure
        """
    )
    
    # Essential arguments
    parser.add_argument('--token', required=True, help='Trend Vision One API token')
    parser.add_argument('--timeframe', type=int, default=7, help='Number of days to look back (default: 7)')
    
    # Status filters (mutually exclusive)
    status_group = parser.add_mutually_exclusive_group()
    status_group.add_argument('--all', action='store_true', help='Include both failures and successes')
    status_group.add_argument('--f', action='store_true', help='Failures only')
    status_group.add_argument('--s', action='store_true', help='Successes only')
    
    # Provider filters (mutually exclusive)
    provider_group = parser.add_mutually_exclusive_group()
    provider_group.add_argument('--aws', action='store_true', help='AWS accounts only')
    provider_group.add_argument('--azure', action='store_true', help='Azure accounts only')
    provider_group.add_argument('--gcp', action='store_true', help='GCP accounts only')
    
    args = parser.parse_args()
    
    # Set default status filter if none specified
    if not any([args.all, args.f, args.s]):
        args.f = True  # Default to failures only
    
    return args

# Parse arguments first
args = parse_arguments()

# Handle status filtering logic
if args.all:
    status_filter = "all"
elif args.s:
    status_filter = "successes"  
elif args.f:
    status_filter = "failures"
else:
    status_filter = "failures"  # Default

# Handle provider filtering logic
if args.aws:
    provider_filter = "aws"
elif args.azure:
    provider_filter = "azure"
elif args.gcp:
    provider_filter = "gcp"
else:
    provider_filter = None  # All providers

# Simple configuration - no complex modes
print(f"📊 Cloud Posture Report - Last {args.timeframe} days")
print(f"Status filter: {status_filter.title()}")
if provider_filter:
    print(f"Provider filter: {provider_filter.upper()} only")
else:
    print(f"Provider filter: All providers (AWS, Azure, GCP)")

# Map arguments to internal settings
DAYS_BACK = args.timeframe
STATUS_FILTER = status_filter
PROVIDER_FILTER = provider_filter

# Setup logging
logging.basicConfig(filename='api_audit.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration based on arguments
BASE_URL = f"https://api.xdr.trendmicro.com"

headers = {
    'Authorization': 'Bearer ' + args.token,
    'Content-Type': 'application/json',
    'api-version': 'v1'
}



def get_accounts():
    account_url = f"{BASE_URL}/beta/cloudPosture/accounts"
    try:
        response = requests.get(account_url, headers=headers)
        logging.info(f"Request to {account_url} - Status Code: {response.status_code}")
        
        if response.status_code == 200:
            accounts_data = response.json()
            # Vision One API uses 'items' not 'data'
            accounts = accounts_data.get('items', [])
            logging.info(f"All accounts fetched: {len(accounts)}")
            
            accounts_list = []
            filtered_count = 0
            
            for account in accounts:
                # Vision One API has flat structure, no 'attributes' wrapper
                provider = account.get('provider', '').lower()
                
                # Apply provider filtering at account level
                if PROVIDER_FILTER and provider != PROVIDER_FILTER:
                    filtered_count += 1
                    continue
                
                account_details = {
                    'id': account.get('id'),
                    'name': account.get('name'),
                    'provider': provider,
                    'aws_account_id': account.get('awsAccountId'),  # Different field name
                    'gcp_project_id': account.get('gcpProjectId'),
                    'azure_subscription_id': account.get('azureSubscriptionId'),
                    'resource_count': account.get('resourcesCount', 0)
                }
                accounts_list.append(account_details)
            
            if PROVIDER_FILTER:
                print(f"📊 Filtered accounts: {len(accounts_list)} {PROVIDER_FILTER.upper()} accounts (skipped {filtered_count} other provider accounts)")
            else:
                print(f"📊 All accounts: {len(accounts_list)} accounts")
            
            logging.info(f"Accounts after filtering: {len(accounts_list)}")
            return accounts_list
        else:
            logging.error(f"Failed to fetch accounts. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def get_checks(account_id, account_name):
    """Get checks for a specific account with timeframe filtering"""
    checks_url = f"{BASE_URL}/beta/cloudPosture/checks"
    
    # Build filter conditions based on mode
    filter_conditions = [f'accountId eq \'{account_id}\'']
    
    if args.all:
        # All mode: only account ID filter, no status filtering
        print(f"[{account_name}] 🎯 ALL MODE: Only account ID filter (all risk levels, both statuses)")
    else:
        # Add status filter (failures only by default)
        if STATUS_FILTER == "failures":
            filter_conditions.append('status eq \'FAILURE\'')
        elif STATUS_FILTER == "successes":
            filter_conditions.append('status eq \'SUCCESS\'')
    
        # Date filtering via query parameters (as per API documentation)
    start_date = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    end_date = datetime.now(timezone.utc)
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    
    # Combine filter conditions (exclude date filtering from TMV1-Filter)
    filter_string = ' and '.join(filter_conditions)
    
    # Query parameters with date filtering - Z format with dateTimeTarget
    params = {
        'top': 200,  # Maximum allowed by API
        'startDateTime': start_date_str.replace('+00:00', 'Z'),
        'endDateTime': end_date_str.replace('+00:00', 'Z'),
        'dateTimeTarget': 'createdDate'
    }
    
    # TMV1-Filter header for non-date filtering only
    headers_with_filter = headers.copy()
    headers_with_filter['TMV1-Filter'] = filter_string
    
    print(f"[{account_name}] 📅 Timeframe: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"[{account_name}] 🔍 TMV1-Filter header: {filter_string}")
    print(f"[{account_name}] 🔍 Query params (with dates): {params}")
    
    all_checks = []
    next_token = None
    page_count = 0
    
    start_time = datetime.now()
    
    next_url = None  # For first request, use the base URL with params
    
    while True:
        page_count += 1
        
        try:
            # HACK: Add date filtering to nextLink URLs since API doesn't preserve them
            if next_url:
                # Add our date parameters to the nextLink URL
                parsed_url = urllib.parse.urlparse(next_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                
                # Add our date filtering parameters
                query_params['startDateTime'] = [start_date_str.replace('+00:00', 'Z')]
                query_params['endDateTime'] = [end_date_str.replace('+00:00', 'Z')]
                query_params['dateTimeTarget'] = ['createdDate']
                
                # Rebuild the URL with our date parameters
                new_query = urllib.parse.urlencode(query_params, doseq=True)
                fixed_next_url = urllib.parse.urlunparse((
                    parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                    parsed_url.params, new_query, parsed_url.fragment
                ))
                
                response = requests.get(fixed_next_url, headers=headers_with_filter, timeout=30)
                print(f"[{account_name}] Page {page_count}: Using FIXED nextLink URL with date filtering")
            else:
                # Use both account filtering AND date filtering 
                response = requests.get(checks_url, headers=headers_with_filter, params=params, timeout=30)
                print(f"[{account_name}] Page {page_count}: Using base URL with account + date filtering")
                # DEBUG: Show exactly what we're sending for the first request
                print(f"[{account_name}] 🔍 First request URL: {response.url}")
                print(f"[{account_name}] 🔍 TMV1-Filter: {filter_string}")
                print(f"[{account_name}] 🔍 Date params: startDateTime={start_date_str.replace('+00:00', 'Z')}, endDateTime={end_date_str.replace('+00:00', 'Z')}")
            
            logging.info(f"Request URL: {response.url} - Status Code: {response.status_code}")
            
            if response.status_code != 200:
                print(f"[{account_name}] Error: HTTP {response.status_code}")
                break
            
            checks_data = response.json()
            checks_items = checks_data.get('items', [])
            
            # Add checks to our collection
            all_checks.extend(checks_items)
            
            print(f"[{account_name}] Page {page_count}: {len(checks_items)} items (Total: {len(all_checks)})")
            
            # Debug: Show pagination fields in response
            if page_count == 1:  # Only show for first page to avoid spam
                pagination_fields = {}
                for field in ['skipToken', 'nextLink', 'count', 'hasMore']:
                    if field in checks_data:
                        if field == 'nextLink':
                            # Show just start of nextLink for debugging
                            link_preview = checks_data[field][:100] + "..." if len(checks_data[field]) > 100 else checks_data[field]
                            pagination_fields[field] = link_preview
                        else:
                            pagination_fields[field] = checks_data[field]
                print(f"[{account_name}] 🔍 Pagination fields: {pagination_fields}")
            
            # Debug: Check if date filtering is working by sampling a few checks
            if checks_items and len(checks_items) > 0:
                sample_size = min(3, len(checks_items))
                print(f"[{account_name}] 📅 Date sample from page {page_count}:")
                for i in range(sample_size):
                    check = checks_items[i]
                    created_date = check.get('createdDateTime', 'No date')
                    rule_id = check.get('ruleId', 'No rule')[:20]  # Truncate for display
                    print(f"[{account_name}]   Check {i+1}: {created_date} ({rule_id})")
                
                # Verify if dates are within our expected range
                filter_start = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
                filter_end = datetime.now(timezone.utc)
                within_range_count = 0
                outside_range_count = 0
                
                for check in checks_items[:10]:  # Check first 10 items
                    created_date = check.get('createdDateTime')
                    if created_date:
                        try:
                            if 'Z' in created_date:
                                check_datetime = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                            else:
                                check_datetime = datetime.fromisoformat(created_date)
                            
                            if filter_start <= check_datetime <= filter_end:
                                within_range_count += 1
                            else:
                                outside_range_count += 1
                        except:
                            pass
                
                if outside_range_count > 0:
                    print(f"[{account_name}] ⚠️ Found {outside_range_count} checks outside {DAYS_BACK}-day range in first 10 items!")
                else:
                    print(f"[{account_name}] ✅ All sampled checks are within {DAYS_BACK}-day range")
            
            # Check for next page using nextLink directly
            next_url = checks_data.get('nextLink')
            
            if not next_url:
                print(f"[{account_name}] ✅ Completed - {len(all_checks)} total checks")
                break
            else:
                print(f"[{account_name}] 🔄 Continuing to page {page_count + 1} with fixed date filtering")
                if len(checks_items) < 200:  # If we got less than max page size, we might be done
                    print(f"[{account_name}] ⚠️ Got {len(checks_items)} items (less than 200), but nextLink exists")
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {account_name}: {e}")
            print(f"[{account_name}] ❌ Request failed: {e}")
            break
    
    elapsed_time = datetime.now() - start_time
    total_checks = len(all_checks)
    
    print(f"[{account_name}] Finished: {total_checks} checks in {elapsed_time.total_seconds():.1f}s")
    logging.info(f"Checks fetched for account {account_name}: {total_checks}")
    
    return {
        'items': all_checks, 
        'account_name': account_name, 
        'account_id': account_id
    }



if __name__ == "__main__":
    start_time = datetime.now()
    accounts_info = get_accounts()
    failures = []
    successes = []
    
    if accounts_info:
        print(f"\nProcessing {len(accounts_info)} accounts:")
        for account in accounts_info:
            print(f"  - {account['name']} ({account['provider'].upper()}) - {account['resource_count']} resources")
        
        # Process each account
        for account in accounts_info:
            print(f"\n🔄 Processing: {account['name']}")
            checks_info = get_checks(account['id'], account['name'])
            
            if checks_info and 'items' in checks_info:
                for check in checks_info['items']:
                    # Create record
                    record = {
                        "account_id": account['id'],
                        "account_name": account['name'],
                        "provider": account['provider'],
                        "aws_account_id": account.get('aws_account_id', ''),
                        "gcp_project_id": account.get('gcp_project_id', ''),
                        "azure_subscription_id": account.get('azure_subscription_id', ''),
                        "resource_count": account['resource_count'],
                        "status": check.get("status"),
                        "rule_id": check.get("ruleId"),
                        "resource_id": check.get("resourceId"),
                        "resource": check.get("resource"),
                        "service": check.get("service"),
                        "region": check.get("region"),
                        "risk_level": check.get("riskLevel"),
                        "categories": ', '.join(check.get("categories", [])) if isinstance(check.get("categories"), list) else check.get("categories", ""),
                        "description": check.get("description"),
                        "status_updated_time": check.get("statusUpdatedDateTime"),
                        "created_time": check.get("createdDateTime"),
                        "failure_discovered_time": check.get("failureDiscoveredDateTime"),
                        "failed_by": check.get("failedBy"),
                        "resolved_time": check.get("resolvedDateTime"),
                        "resolved_by": check.get("resolvedBy")
                    }
                    
                    if check.get("status") == "SUCCESS":
                        successes.append(record)
                    elif check.get("status") == "FAILURE":
                        failures.append(record)

        elapsed_time = datetime.now() - start_time
        total_checks = len(failures) + len(successes)
        
        print(f"\n=== SUMMARY ===")
        print(f"⏱️ Processing time: {elapsed_time.total_seconds():.1f} seconds")
        print(f"📊 Total checks: {total_checks}")
        print(f"✅ Successes: {len(successes)}")
        print(f"❌ Failures: {len(failures)}")

        # Export to Excel
        output_file = f"compliance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            if failures:
                df_failures = pd.DataFrame(failures)
                df_failures.to_excel(writer, sheet_name='Failures', index=False)
            
            if successes:
                df_successes = pd.DataFrame(successes)
                df_successes.to_excel(writer, sheet_name='Successes', index=False)
            
            if not failures and not successes:
                empty_df = pd.DataFrame({'Message': ['No data found with current filters']})
                empty_df.to_excel(writer, sheet_name='No Data', index=False)

        print(f"📄 Report saved: {output_file}")
    else:
        print("❌ Failed to retrieve account information. Check your API token and permissions.")
