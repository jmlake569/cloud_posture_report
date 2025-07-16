import requests
import logging
import pandas as pd
import argparse
import json
import time
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Optional, Set
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Generate Cloud Posture compliance report (optimized for 50+ accounts)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python report.py --token "your_token_here" --timeframe 7 --all
  python report.py --token "your_token_here" --timeframe 30 --f --aws --max-workers 10
  python report.py --token "your_token_here" --timeframe 7 --s --azure --resume
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
    
    # Performance and reliability options
    parser.add_argument('--max-workers', type=int, default=5, help='Max concurrent account processing (default: 5)')
    parser.add_argument('--resume', action='store_true', help='Resume from previous checkpoint')
    parser.add_argument('--batch-size', type=int, default=1000, help='Records per output batch (default: 1000)')
    
    args = parser.parse_args()
    
    # Set default status filter if none specified
    if not any([args.all, args.f, args.s]):
        args.f = True  # Default to failures only
    
    return args

class RateLimitHandler:
    """Handle API rate limiting with exponential backoff per Trend Micro documentation"""
    
    def __init__(self, max_retries=5):
        self.max_retries = max_retries
        self.rate_limit_lock = threading.Lock()
        self.last_rate_limit_time = 0
        
    def handle_rate_limit(self, retries: int) -> bool:
        """Returns True if should retry, False if max retries exceeded"""
        if retries >= self.max_retries:
            return False
            
        # Exponential backoff as per Trend Micro documentation
        exp_backoff = (2 ** (retries + 3)) / 1000
        
        with self.rate_limit_lock:
            # Add some jitter to prevent thundering herd
            jitter = 0.1 * exp_backoff * (time.time() % 1)
            sleep_time = exp_backoff + jitter
            
            print(f"⏳ API rate limit exceeded. Retrying in {sleep_time:.2f}s (attempt {retries + 1}/{self.max_retries})")
            time.sleep(sleep_time)
            
        return True

class ProgressTracker:
    """Track and persist progress for resume capability"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.progress_file = f"progress_{session_id}.json"
        self.completed_accounts: Set[str] = set()
        self.failed_accounts: Set[str] = set()
        self.total_checks = 0
        self.start_time = datetime.now()
        
        # Load existing progress if resuming
        self.load_progress()
        
    def load_progress(self):
        """Load progress from checkpoint file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    self.completed_accounts = set(data.get('completed_accounts', []))
                    self.failed_accounts = set(data.get('failed_accounts', []))
                    self.total_checks = data.get('total_checks', 0)
                    print(f"📂 Resumed session: {len(self.completed_accounts)} accounts completed, {len(self.failed_accounts)} failed")
            except Exception as e:
                print(f"⚠️ Could not load progress file: {e}")
                
    def save_progress(self):
        """Save current progress to checkpoint file"""
        data = {
            'session_id': self.session_id,
            'completed_accounts': list(self.completed_accounts),
            'failed_accounts': list(self.failed_accounts),
            'total_checks': self.total_checks,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"⚠️ Could not save progress: {e}")
            
    def mark_completed(self, account_id: str, check_count: int):
        """Mark account as completed"""
        self.completed_accounts.add(account_id)
        self.total_checks += check_count
        self.save_progress()
        
    def mark_failed(self, account_id: str):
        """Mark account as failed"""
        self.failed_accounts.add(account_id)
        self.save_progress()
        
    def is_completed(self, account_id: str) -> bool:
        """Check if account already completed"""
        return account_id in self.completed_accounts
        
    def cleanup(self):
        """Remove progress file after successful completion"""
        try:
            if os.path.exists(self.progress_file):
                os.remove(self.progress_file)
        except:
            pass

class StreamingReporter:
    """Memory-efficient streaming reporter that writes data incrementally"""
    
    def __init__(self, session_id: str, batch_size: int = 1000, timeframe: int = 7, status_filter: str = "failures", provider_filter: str = None):
        self.session_id = session_id
        self.batch_size = batch_size
        self.timeframe = timeframe
        self.status_filter = status_filter
        self.provider_filter = provider_filter
        self.output_dir = Path(f"report_data_{session_id}")
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize batch counters
        self.failures_batch = 0
        self.successes_batch = 0
        self.failures_buffer = []
        self.successes_buffer = []
        
    def add_record(self, record: Dict, status: str):
        """Add a record to the appropriate buffer"""
        if status == "FAILURE":
            self.failures_buffer.append(record)
            if len(self.failures_buffer) >= self.batch_size:
                self._flush_failures()
        elif status == "SUCCESS":
            self.successes_buffer.append(record)
            if len(self.successes_buffer) >= self.batch_size:
                self._flush_successes()
                
    def _flush_failures(self):
        """Write failures buffer to file"""
        if self.failures_buffer:
            filename = self.output_dir / f"failures_batch_{self.failures_batch:03d}.csv"
            df = pd.DataFrame(self.failures_buffer)
            df.to_csv(filename, index=False)
            
            count = len(self.failures_buffer)
            print(f"💾 Saved {count} failures to {filename}")
            
            self.failures_buffer.clear()
            self.failures_batch += 1
            
    def _flush_successes(self):
        """Write successes buffer to file"""
        if self.successes_buffer:
            filename = self.output_dir / f"successes_batch_{self.successes_batch:03d}.csv"
            df = pd.DataFrame(self.successes_buffer)
            df.to_csv(filename, index=False)
            
            count = len(self.successes_buffer)
            print(f"💾 Saved {count} successes to {filename}")
            
            self.successes_buffer.clear()
            self.successes_batch += 1
            
    def finalize(self) -> str:
        """Flush remaining data and combine into final Excel report"""
        # Flush any remaining data
        self._flush_failures()
        self._flush_successes()
        
        # Combine all batch files into final Excel report
        output_file = f"compliance_report_{self.session_id}.xlsx"
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Combine failures
            failure_files = list(self.output_dir.glob("failures_batch_*.csv"))
            if failure_files:
                failure_dfs = [pd.read_csv(f) for f in failure_files]
                combined_failures = pd.concat(failure_dfs, ignore_index=True)
                combined_failures.to_excel(writer, sheet_name='Failures', index=False)
                print(f"📊 Combined {len(combined_failures)} failures")
            
            # Combine successes
            success_files = list(self.output_dir.glob("successes_batch_*.csv"))
            if success_files:
                success_dfs = [pd.read_csv(f) for f in success_files]
                combined_successes = pd.concat(success_dfs, ignore_index=True)
                combined_successes.to_excel(writer, sheet_name='Successes', index=False)
                print(f"📊 Combined {len(combined_successes)} successes")
            
            # Create summary sheet with timeframe information
            total_failures = len(pd.concat([pd.read_csv(f) for f in failure_files], ignore_index=True)) if failure_files else 0
            total_successes = len(pd.concat([pd.read_csv(f) for f in success_files], ignore_index=True)) if success_files else 0
            
            # Create explanatory text for what the timeframe means
            if self.status_filter == "failures":
                timeframe_meaning = f"Current failures discovered/updated in last {self.timeframe} days"
            elif self.status_filter == "successes":
                timeframe_meaning = f"Issues resolved in last {self.timeframe} days"
            else:  # all
                timeframe_meaning = f"Current failures AND recent fixes from last {self.timeframe} days"
            
            summary_data = {
                'Metric': [
                    'Report Timeframe (Days)',
                    'Data Meaning',
                    'Report Generated',
                    'Session ID',
                    '',  # Separator
                    'Total Failures', 
                    'Total Successes', 
                    'Total Items',
                    '',  # Separator
                    'Status Filter',
                    'Provider Filter'
                ],
                'Value': [
                    f"{self.timeframe} days",
                    timeframe_meaning,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    self.session_id,
                    '',  # Separator
                    total_failures,
                    total_successes,
                    total_failures + total_successes,
                    '',  # Separator
                    self.status_filter.title(),
                    self.provider_filter.upper() if self.provider_filter else 'All Providers'
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Cleanup temporary files
        for f in self.output_dir.glob("*.csv"):
            f.unlink()
        self.output_dir.rmdir()
        
        return output_file

# Parse arguments first
args = parse_arguments()

# Configuration
session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
BASE_URL = "https://api.xdr.trendmicro.com"

headers = {
    'Authorization': 'Bearer ' + args.token,
    'Content-Type': 'application/json',
    'api-version': 'v1'
}

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

# Initialize components
rate_limiter = RateLimitHandler()
progress_tracker = ProgressTracker(session_id)
streaming_reporter = StreamingReporter(session_id, args.batch_size, args.timeframe, status_filter, provider_filter)

# Setup logging
logging.basicConfig(
    filename=f'api_audit_{session_id}.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

print(f"🚀 Cloud Posture Report (Session: {session_id})")
print(f"📊 Optimized for 50+ accounts with {args.max_workers} concurrent workers")

# Explain what we're actually tracking based on status filter
if status_filter == "successes":
    print(f"🔧 Focus: Issues RESOLVED in last {args.timeframe} days (SUCCESS status)")
elif status_filter == "failures":
    print(f"🔧 Focus: Current FAILURES from last {args.timeframe} days (FAILURE status)")
else:  # all
    print(f"🔧 Focus: Recent fixes AND current failures from last {args.timeframe} days")
print(f"Status filter: {status_filter.title()}")
if provider_filter:
    print(f"Provider filter: {provider_filter.upper()} only")
else:
    print(f"Provider filter: All providers (AWS, Azure, GCP)")

if args.resume:
    print(f"🔄 Resume mode: Skipping {len(progress_tracker.completed_accounts)} completed accounts")

def get_accounts():
    """Fetch all accounts with rate limiting"""
    account_url = f"{BASE_URL}/beta/cloudPosture/accounts"
    retries = 0
    
    while True:
        try:
            response = requests.get(account_url, headers=headers, timeout=30)
            logging.info(f"Request to {account_url} - Status Code: {response.status_code}")
            
            if response.status_code == 200:
                accounts_data = response.json()
                accounts = accounts_data.get('items', [])
                logging.info(f"All accounts fetched: {len(accounts)}")
                
                accounts_list = []
                filtered_count = 0
                
                for account in accounts:
                    provider = account.get('provider', '').lower()
                    
                    # Apply provider filtering at account level
                    if provider_filter and provider != provider_filter:
                        filtered_count += 1
                        continue
                    
                    account_details = {
                        'id': account.get('id'),
                        'name': account.get('name'),
                        'provider': provider,
                        'aws_account_id': account.get('awsAccountId'),
                        'gcp_project_id': account.get('gcpProjectId'),
                        'azure_subscription_id': account.get('azureSubscriptionId'),
                        'resource_count': account.get('resourcesCount', 0)
                    }
                    accounts_list.append(account_details)
                
                if provider_filter:
                    print(f"📊 Filtered accounts: {len(accounts_list)} {provider_filter.upper()} accounts (skipped {filtered_count} other provider accounts)")
                else:
                    print(f"📊 All accounts: {len(accounts_list)} accounts")
                
                return accounts_list
                
            elif response.status_code == 429:
                # Rate limit exceeded
                if rate_limiter.handle_rate_limit(retries):
                    retries += 1
                    continue
                else:
                    logging.error(f"Max retries exceeded for accounts fetch")
                    return None
            else:
                logging.error(f"Failed to fetch accounts. Status code: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            if retries < rate_limiter.max_retries:
                retries += 1
                time.sleep(2 ** retries)  # Simple backoff for network errors
                continue
            return None

def get_checks_for_account(account: Dict) -> Dict:
    """Get checks for a specific account with rate limiting and error handling"""
    account_id = account['id']
    account_name = account['name']
    
    # Skip if already completed and resuming
    if args.resume and progress_tracker.is_completed(account_id):
        print(f"⏭️  Skipping {account_name} (already completed)")
        return {'items': [], 'account_name': account_name, 'account_id': account_id, 'skipped': True}
    
    checks_url = f"{BASE_URL}/beta/cloudPosture/checks"
    
    # Build filter conditions
    filter_conditions = [f'accountId eq \'{account_id}\'']
    
    if not args.all:
        if status_filter == "failures":
            filter_conditions.append('status eq \'FAILURE\'')
        elif status_filter == "successes":
            filter_conditions.append('status eq \'SUCCESS\'')
    
    # Date filtering
    start_date = datetime.now(timezone.utc) - timedelta(days=args.timeframe)
    end_date = datetime.now(timezone.utc)
    start_date_str = start_date.isoformat().replace('+00:00', 'Z')
    end_date_str = end_date.isoformat().replace('+00:00', 'Z')
    
    filter_string = ' and '.join(filter_conditions)
    
    params = {
        'top': 200,
        'startDateTime': start_date_str,
        'endDateTime': end_date_str,
        'dateTimeTarget': 'createdDate'
    }
    
    headers_with_filter = headers.copy()
    headers_with_filter['TMV1-Filter'] = filter_string
    
    all_checks = []
    page_count = 0
    next_url = None
    retries = 0
    
    print(f"🔄 [{account_name}] Starting data collection...")
    
    while True:
        page_count += 1
        
        try:
            if next_url:
                # Add date parameters to nextLink URL
                parsed_url = urllib.parse.urlparse(next_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                query_params.update({
                    'startDateTime': [start_date_str],
                    'endDateTime': [end_date_str],
                    'dateTimeTarget': ['createdDate']
                })
                new_query = urllib.parse.urlencode(query_params, doseq=True)
                fixed_next_url = urllib.parse.urlunparse((
                    parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                    parsed_url.params, new_query, parsed_url.fragment
                ))
                response = requests.get(fixed_next_url, headers=headers_with_filter, timeout=30)
            else:
                response = requests.get(checks_url, headers=headers_with_filter, params=params, timeout=30)
            
            logging.info(f"[{account_name}] Page {page_count} - Status Code: {response.status_code}")
            
            if response.status_code == 200:
                checks_data = response.json()
                checks_items = checks_data.get('items', [])
                all_checks.extend(checks_items)
                
                print(f"📄 [{account_name}] Page {page_count}: +{len(checks_items)} items (Total: {len(all_checks)})")
                
                next_url = checks_data.get('nextLink')
                if not next_url:
                    break
                    
                retries = 0  # Reset retries on successful request
                
            elif response.status_code == 429:
                # Rate limit exceeded
                if rate_limiter.handle_rate_limit(retries):
                    retries += 1
                    continue
                else:
                    logging.error(f"[{account_name}] Max retries exceeded")
                    progress_tracker.mark_failed(account_id)
                    return {'items': [], 'account_name': account_name, 'account_id': account_id, 'error': 'rate_limit_exceeded'}
            else:
                logging.error(f"[{account_name}] HTTP Error: {response.status_code}")
                progress_tracker.mark_failed(account_id)
                return {'items': [], 'account_name': account_name, 'account_id': account_id, 'error': f'http_{response.status_code}'}
                
        except requests.exceptions.RequestException as e:
            logging.error(f"[{account_name}] Request failed: {e}")
            if retries < rate_limiter.max_retries:
                retries += 1
                time.sleep(2 ** retries)
                continue
            else:
                progress_tracker.mark_failed(account_id)
                return {'items': [], 'account_name': account_name, 'account_id': account_id, 'error': str(e)}
    
    print(f"✅ [{account_name}] Completed: {len(all_checks)} checks")
    return {'items': all_checks, 'account_name': account_name, 'account_id': account_id}

def process_checks_for_account(account: Dict, checks_info: Dict):
    """Process checks and stream to reporter with corrected logic"""
    if checks_info.get('skipped') or checks_info.get('error'):
        return 0, 0
    
    checks = checks_info.get('items', [])
    if not checks:
        return 0, 0
    
    account_name = account['name']
    successes_count = 0
    failures_count = 0
    
    # Define the timeframe for filtering
    filter_start = datetime.now(timezone.utc) - timedelta(days=args.timeframe)
    filter_end = datetime.now(timezone.utc)
    
    for check in checks:
        status = check.get("status")
        include_check = False
        
        # Logic based on what we're actually trying to track:
        if status == "SUCCESS":
            # For SUCCESS items: Include if recently resolved (fixed issues)
            resolved_date_str = check.get('resolvedDateTime')
            if resolved_date_str:
                try:
                    if 'Z' in resolved_date_str:
                        resolved_date = datetime.fromisoformat(resolved_date_str.replace('Z', '+00:00'))
                    else:
                        resolved_date = datetime.fromisoformat(resolved_date_str)
                    
                    if filter_start <= resolved_date <= filter_end:
                        include_check = True
                except:
                    pass
        
        elif status == "FAILURE":
            # For FAILURE items: Include if currently failing (discovered/updated recently)
            # Check multiple date fields to catch active failures
            date_fields = ['failureDiscoveredDateTime', 'statusUpdatedDateTime', 'updatedDateTime']
            
            for date_field in date_fields:
                date_str = check.get(date_field)
                if date_str:
                    try:
                        if 'Z' in date_str:
                            check_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        else:
                            check_date = datetime.fromisoformat(date_str)
                        
                        if filter_start <= check_date <= filter_end:
                            include_check = True
                            break
                    except:
                        continue
        
        if not include_check:
            continue
        
        # Create comprehensive record with all available API fields
        record = {
            # Check identifiers
            "check_id": check.get("id"),
            "account_id": account['id'],
            "account_name": account['name'],
            "organization_id": check.get("organizationId"),
            
            # Cloud provider info
            "provider": account['provider'],
            "aws_account_id": account.get('aws_account_id', ''),
            "gcp_project_id": account.get('gcp_project_id', ''),
            "azure_subscription_id": account.get('azure_subscription_id', ''),
            "resource_count": account['resource_count'],
            
            # Check status and timing
            "status": status,
            "created_time": check.get("createdDateTime"),
            "updated_time": check.get("updatedDateTime"),
            "status_updated_time": check.get("statusUpdatedDateTime"),
            "failure_discovered_time": check.get("failureDiscoveredDateTime"),
            "resolved_time": check.get("resolvedDateTime"),
            
            # Rule and resource details
            "rule_id": check.get("ruleId"),
            "rule_title": check.get("ruleTitle"),
            "is_custom_rule": check.get("isCustom", False),
            "resource_id": check.get("resourceId"),
            "resource": check.get("resource"),
            "resource_name": check.get("resourceName"),
            "resource_type": check.get("resourceType"),
            "resource_link": check.get("resourceLink"),
            "service": check.get("service"),
            "region": check.get("region"),
            
            # Risk and compliance
            "risk_level": check.get("riskLevel"),
            "categories": ', '.join(check.get("categories", [])) if isinstance(check.get("categories"), list) else check.get("categories", ""),
            "compliances": ', '.join(check.get("compliances", [])) if isinstance(check.get("compliances"), list) else check.get("compliances", ""),
            "description": check.get("description"),
            "resolution_page_url": check.get("resolutionPageUrl"),
            
            # Management fields
            "failed_by": check.get("failedBy"),
            "resolved_by": check.get("resolvedBy"),
            "note": check.get("note"),
            "suppressed": check.get("suppressed", False),
            "suppressed_until": check.get("suppressedUntilDateTime"),
            "excluded": check.get("excluded", False),
            "ignored": check.get("ignored", False),
            "not_scored": check.get("notScored", False),
            
            # Additional metadata
            "tags": ', '.join(check.get("tags", [])) if isinstance(check.get("tags"), list) else check.get("tags", ""),
            "event_id": check.get("eventId"),
            "ttl_time": check.get("ttlDateTime"),
            
            # Extra data (flatten if present)
            "extra_data": json.dumps(check.get("extraData", [])) if check.get("extraData") else ""
        }
        
        streaming_reporter.add_record(record, status)
        
        if status == "SUCCESS":
            successes_count += 1
        elif status == "FAILURE":
            failures_count += 1
    
    return successes_count, failures_count

def main():
    start_time = datetime.now()
    
    # Get all accounts
    print("\n🔍 Fetching accounts...")
    accounts_info = get_accounts()
    
    if not accounts_info:
        print("❌ Failed to retrieve account information. Check your API token and permissions.")
        return
    
    # Filter out already completed accounts if resuming
    if args.resume:
        accounts_to_process = [acc for acc in accounts_info if not progress_tracker.is_completed(acc['id'])]
        print(f"📝 Processing {len(accounts_to_process)} remaining accounts (skipping {len(accounts_info) - len(accounts_to_process)} completed)")
    else:
        accounts_to_process = accounts_info
    
    if not accounts_to_process:
        print("✅ All accounts already processed!")
        return
    
    print(f"\n🏭 Processing {len(accounts_to_process)} accounts with {args.max_workers} workers:")
    for account in accounts_to_process:
        print(f"  - {account['name']} ({account['provider'].upper()}) - {account['resource_count']} resources")
    
    total_successes = 0
    total_failures = 0
    processed_count = 0
    
    # Process accounts concurrently
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        # Submit all account processing jobs
        future_to_account = {
            executor.submit(get_checks_for_account, account): account 
            for account in accounts_to_process
        }
        
        # Process results as they complete
        for future in as_completed(future_to_account):
            account = future_to_account[future]
            processed_count += 1
            
            try:
                checks_info = future.result()
                
                # Process the checks and stream to reporter
                successes, failures = process_checks_for_account(account, checks_info)
                total_successes += successes
                total_failures += failures
                
                # Mark as completed if successful
                if not checks_info.get('error'):
                    progress_tracker.mark_completed(account['id'], successes + failures)
                
                # Progress update
                elapsed = datetime.now() - start_time
                rate = processed_count / elapsed.total_seconds() * 60  # accounts per minute
                remaining = len(accounts_to_process) - processed_count
                eta_minutes = remaining / rate if rate > 0 else 0
                
                print(f"📈 Progress: {processed_count}/{len(accounts_to_process)} accounts ({processed_count/len(accounts_to_process)*100:.1f}%) | "
                      f"Rate: {rate:.1f} acc/min | ETA: {eta_minutes:.1f} min | "
                      f"✅ {successes} ❌ {failures}")
                      
            except Exception as e:
                logging.error(f"Error processing {account['name']}: {e}")
                progress_tracker.mark_failed(account['id'])
                print(f"❌ [{account['name']}] Processing failed: {e}")
    
    # Finalize report
    elapsed_time = datetime.now() - start_time
    print(f"\n🏁 Processing completed in {elapsed_time.total_seconds():.1f} seconds")
    print(f"📊 Total processed: {processed_count} accounts")
    print(f"✅ Successes: {total_successes}")
    print(f"❌ Failures: {total_failures}")
    print(f"📈 Rate: {processed_count / elapsed_time.total_seconds() * 60:.1f} accounts/minute")
    
    # Generate final report
    print(f"\n📄 Generating final report...")
    output_file = streaming_reporter.finalize()
    print(f"📄 Report saved: {output_file}")
    
    # Cleanup
    progress_tracker.cleanup()
    print(f"✨ Session {session_id} completed successfully!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n⚠️ Interrupted! Progress saved. Use --resume to continue.")
        progress_tracker.save_progress()
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        logging.error(f"Unexpected error: {e}")
        progress_tracker.save_progress()
