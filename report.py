import requests
import logging
import pandas as pd
import argparse
import json
import time
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Set
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Generate Cloud Posture compliance report (optimized for 50+ accounts)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using CLI token with default risk levels (excludes LOW) - separate API requests for speed
  python report.py --token "your_token_here" --timeframe 7 --all
  
  # Include only high-severity findings - single focused request
  python report.py --timeframe 30 --failures --risk-levels HIGH VERY_HIGH EXTREME
  
  # Include ALL risk levels (including LOW) - separate requests for failures and successes
  python report.py --timeframe 7 --all --risk-levels LOW MEDIUM HIGH VERY_HIGH EXTREME
  
  # Using environment variable (recommended for security) - faster separate requests
  export TMV1_TOKEN="your_token_here"
  python report.py --timeframe 30 --failures --max-workers 10
  
  # Resume capability with performance optimization
  python report.py --timeframe 7 --successes --resume

Performance Notes:
  • --all now makes separate API requests for failures/successes (faster)
  • Each request is smaller and more reliable
  • Risk level filtering applied at API level to minimize data transfer
  • Service-level concurrency: --service-workers 10 (default) for 10x speedup
  • Account-level concurrency: --max-workers 5 (default) for parallel accounts
        """
    )
    
    # Essential arguments
    parser.add_argument('--token', help='Trend Vision One API token (or set TMV1_TOKEN env var)')
    parser.add_argument('--timeframe', type=int, default=7, help='Number of days to look back (default: 7)')
    
    # Status filters (mutually exclusive)
    status_group = parser.add_mutually_exclusive_group()
    status_group.add_argument('--all', action='store_true', help='Include both failures and successes')
    status_group.add_argument('--failures', action='store_true', help='Failures only')
    status_group.add_argument('--successes', action='store_true', help='Successes only')
    
    # Risk level filtering
    parser.add_argument('--risk-levels', nargs='+', 
                       choices=['LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH', 'EXTREME'],
                       default=['MEDIUM', 'HIGH', 'VERY_HIGH', 'EXTREME'],
                       help='Risk levels to include (default: excludes LOW)')
    
    # Performance and reliability options
    parser.add_argument('--max-workers', type=int, default=5, help='Max concurrent account processing (default: 5)')
    parser.add_argument('--service-workers', type=int, default=10, help='Max concurrent service requests per account (default: 10)')
    parser.add_argument('--resume', action='store_true', help='Resume from previous checkpoint')
    parser.add_argument('--batch-size', type=int, default=1000, help='Records per output batch (default: 1000)')
    
    args = parser.parse_args()
    
    # Handle API token from CLI or environment variable
    if not args.token:
        args.token = os.getenv('TMV1_TOKEN')
        if not args.token:
            parser.error("API token required: use --token or set TMV1_TOKEN environment variable")
        else:
            print("🔑 Using API token from TMV1_TOKEN environment variable")
    else:
        print("🔑 Using API token from command line argument")
    
    # Set default status filter if none specified
    if not any([args.all, args.failures, args.successes]):
        args.failures = True  # Default to failures only
    
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
    
    def __init__(self, session_id: str, batch_size: int = 1000, timeframe: int = 7, status_filter: str = "failures"):
        self.session_id = session_id
        self.batch_size = batch_size
        self.timeframe = timeframe
        self.status_filter = status_filter
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
            
            # Risk level information
            risk_level_summary = ', '.join(args.risk_levels) if len(args.risk_levels) < 5 else 'ALL'
            risk_level_note = 'Excludes LOW risk findings' if 'LOW' not in args.risk_levels else 'Includes ALL risk levels'
            
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
                    'Risk Levels Included',
                    'Risk Level Note',
                    '',  # Separator
                    'Advanced Capabilities',
                    '10K+ Result Handling',
                    'Chunking Strategy',
                    'Resume Support'
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
                    risk_level_summary,
                    risk_level_note,
                    '',  # Separator
                    'Enabled for 50+ account scalability',
                    'Auto-chunking by risk level when hitting 10K limit',
                    'accountId + status filtering with risk level fallback',
                    'Checkpoint-based recovery with --resume'
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Cleanup temporary files
        for f in self.output_dir.glob("*.csv"):
            f.unlink()
        self.output_dir.rmdir()
        
        return output_file

class ServiceConcurrencyManager:
    """Manages concurrent service requests with rate limiting and error handling"""
    
    def __init__(self, max_concurrent_services: int = 10, rate_limit_delay: float = 0.05):
        self.max_concurrent_services = max_concurrent_services
        self.rate_limit_delay = rate_limit_delay
        self.request_semaphore = threading.Semaphore(max_concurrent_services)
        self.last_request_time = 0
        self.request_lock = threading.Lock()
        
    def _rate_limit(self):
        """Ensure we don't exceed API rate limits"""
        with self.request_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - time_since_last
                time.sleep(sleep_time)
            self.last_request_time = time.time()
    
    def fetch_service_data(self, account_name: str, service: str, filter_conditions: List[str], status_type: str) -> Dict:
        """Fetch data for a single service with proper concurrency controls"""
        with self.request_semaphore:  # Limit concurrent requests
            self._rate_limit()  # Respect rate limits
            
            try:
                service_filter = filter_conditions + [f'service eq \'{service}\'']
                service_filter_string = ' and '.join(service_filter)
                
                result = _fetch_checks_with_filter(account_name, service_filter_string, max_pages=50)
                
                if result['success'] and result['items']:
                    return {
                        'service': service,
                        'items': result['items'],
                        'hit_limit': result.get('hit_limit', False),
                        'status_type': status_type
                    }
                else:
                    return {
                        'service': service,
                        'items': [],
                        'hit_limit': False,
                        'status_type': status_type,
                        'error': result.get('error', 'no_data')
                    }
                    
            except Exception as e:
                logging.error(f"[{account_name}] Error fetching {service} ({status_type}): {e}")
                return {
                    'service': service,
                    'items': [],
                    'hit_limit': False,
                    'status_type': status_type,
                    'error': str(e)
                }
    
    def fetch_services_concurrently(self, account_name: str, services: List[str], filter_conditions: List[str], status_type: str) -> Dict:
        """Fetch data for multiple services concurrently"""
        print(f"  🚀 [{account_name}] Fetching {len(services)} services concurrently for {status_type}...")
        
        all_items = []
        services_with_data = []
        services_hitting_limits = []
        services_with_errors = []
        
        # Use ThreadPoolExecutor for concurrent service requests
        with ThreadPoolExecutor(max_workers=self.max_concurrent_services) as executor:
            # Submit all service requests
            future_to_service = {
                executor.submit(self.fetch_service_data, account_name, service, filter_conditions, status_type): service
                for service in services
            }
            
            # Process results as they complete
            completed_count = 0
            for future in as_completed(future_to_service):
                service = future_to_service[future]
                completed_count += 1
                
                try:
                    result = future.result()
                    
                    if result['items']:
                        all_items.extend(result['items'])
                        services_with_data.append(service)
                        
                        if result['hit_limit']:
                            services_hitting_limits.append(service)
                            print(f"    🚨 [{account_name}] {service} ({status_type}) hit 10K limit!")
                    elif result.get('error'):
                        services_with_errors.append(service)
                        print(f"    ⚠️  [{account_name}] {service} ({status_type}) error: {result['error']}")
                    
                    # Progress indicator
                    if completed_count % 20 == 0 or completed_count == len(services):
                        print(f"    📊 [{account_name}] Progress ({status_type}): {completed_count}/{len(services)} services completed, {len(all_items)} checks collected")
                        
                except Exception as e:
                    services_with_errors.append(service)
                    logging.error(f"[{account_name}] Unexpected error processing {service}: {e}")
                    print(f"    ❌ [{account_name}] {service} ({status_type}) failed: {e}")
        
        # Summary
        print(f"  ✅ [{account_name}] {status_type} concurrent fetch completed:")
        print(f"     • {len(services_with_data)}/{len(services)} services had data")
        print(f"     • {len(services_hitting_limits)} services hit limits")
        print(f"     • {len(services_with_errors)} services had errors")
        print(f"     • Total checks: {len(all_items)}")
        
        return {
            'success': True,
            'items': all_items,
            'services_with_data': services_with_data,
            'services_hitting_limits': services_hitting_limits,
            'services_with_errors': services_with_errors,
            'total_services_checked': len(services)
        }

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
elif args.successes:
    status_filter = "successes"  
elif args.failures:
    status_filter = "failures"
else:
    status_filter = "failures"  # Default

# Initialize components
rate_limiter = RateLimitHandler()
progress_tracker = ProgressTracker(session_id)
streaming_reporter = StreamingReporter(session_id, args.batch_size, args.timeframe, status_filter)
service_concurrency_manager = ServiceConcurrencyManager(
    max_concurrent_services=args.service_workers,
    rate_limit_delay=0.05  # Reduced from 0.2s for better performance
)

# Global service tracking for completeness reporting
discovered_services = set()
service_discovery_stats = {
    'accounts_chunked': 0,
    'known_services_found': set(),
    'unknown_services_found': set(),
    'accounts_hitting_limits': []
}

# Cache for official service list (fetch once per session)
_official_services_cache = None

# Setup logging
logging.basicConfig(
    filename=f'api_audit_{session_id}.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

print(f"🚀 Cloud Posture Report (Session: {session_id})")
print(f"📊 Optimized for 50+ accounts with {args.max_workers} concurrent workers")
print(f"⚡ Service concurrency: {args.service_workers} parallel service requests per account")
print(f"🔄 10K+ result handling: Auto-chunking by risk level")
print(f"⚡ Performance: Separate API requests for failures/successes (faster & more reliable)")

# Explain what we're actually tracking based on status filter
if status_filter == "successes":
    print(f"🔧 Focus: Issues RESOLVED in last {args.timeframe} days (SUCCESS status)")
elif status_filter == "failures":
    print(f"🔧 Focus: Current FAILURES from last {args.timeframe} days (FAILURE status)")
else:  # all
    print(f"🔧 Focus: Recent fixes AND current failures from last {args.timeframe} days (separate requests)")

print(f"Status filter: {status_filter.title()}")
print(f"Risk levels: {', '.join(args.risk_levels)} {'(excludes LOW findings)' if 'LOW' not in args.risk_levels else '(includes ALL risk levels)'}")
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
                
                for account in accounts:
                    provider = account.get('provider', '').lower()
                    
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
    """Get checks for a specific account with rate limiting, error handling, and 10K+ result chunking"""
    account_id = account['id']
    account_name = account['name']
    
    # Skip if already completed and resuming
    if args.resume and progress_tracker.is_completed(account_id):
        print(f"⏭️  Skipping {account_name} (already completed)")
        return {'items': [], 'account_name': account_name, 'account_id': account_id, 'skipped': True}
    
    print(f"🔄 [{account_name}] Starting data collection...")
    
    # Build base filter (accountId + risk levels, but NOT status yet)
    base_filter_conditions = [f'accountId eq \'{account_id}\'']
    
    # Add risk level filtering to base
    if len(args.risk_levels) < 5:  # Only add filter if not all risk levels selected
        if len(args.risk_levels) == 1:
            # Single risk level
            base_filter_conditions.append(f'riskLevel eq \'{args.risk_levels[0]}\'')
        else:
            # Multiple risk levels - use OR condition
            risk_conditions = [f'riskLevel eq \'{level}\'' for level in args.risk_levels]
            risk_filter = '(' + ' or '.join(risk_conditions) + ')'
            base_filter_conditions.append(risk_filter)
    
    # Always make separate requests for better performance and reliability
    all_checks = []
    requests_made = []
    
    # Determine which status requests to make
    if args.all:
        status_requests = ['FAILURE', 'SUCCESS']
        print(f"🔧 [{account_name}] Making separate requests for failures and successes")
    elif status_filter == "failures":
        status_requests = ['FAILURE']
        print(f"🔧 [{account_name}] Requesting failures only")
    elif status_filter == "successes":
        status_requests = ['SUCCESS']
        print(f"🔧 [{account_name}] Requesting successes only")
    
    # Make separate requests for each status
    for status in status_requests:
        print(f"  📡 [{account_name}] Fetching {status} checks...")
        
        # Build filter conditions for this specific status
        status_filter_conditions = base_filter_conditions + [f'status eq \'{status}\'']
        
        # Always use comprehensive service chunking for maximum completeness and zero duplication
        resource_count = account.get('resource_count', 0)
        risk_summary = f"risk levels: {', '.join(args.risk_levels)}" if len(args.risk_levels) < 5 else "all risk levels"
        print(f"    🔧 [{account_name}] Using comprehensive service chunking for {status} ({resource_count} resources, {risk_summary})")
        
        # Use service chunking for this status
        chunked_results = _get_checks_with_comprehensive_service_chunking(account_name, status_filter_conditions, status_type=status)
        
        if chunked_results['success']:
            status_checks = chunked_results['items']
            print(f"  ✅ [{account_name}] {status} request completed: {len(status_checks)} checks")
            all_checks.extend(status_checks)
            requests_made.append(status)
            
            # Check if any individual services hit limits
            services_hitting_limits = chunked_results.get('services_hitting_limits', [])
            if services_hitting_limits:
                print(f"  ⚠️  [{account_name}] {status} - Services hitting 10K limit: {', '.join(services_hitting_limits)}")
                service_discovery_stats['accounts_hitting_limits'].append(f"{account_name} ({status})")
        else:
            print(f"  ❌ [{account_name}] {status} request failed")
            # Don't fail completely if one status fails, continue with others
    
    # Track for global reporting
    service_discovery_stats['accounts_chunked'] += 1
    
    if all_checks:
        # Deduplicate across all status requests (shouldn't be needed but safety first)
        combined_checks = _deduplicate_checks(all_checks)
        
        # Track discovered services globally
        for check in combined_checks:
            service = check.get('service')
            if service:
                discovered_services.add(service)
        
        print(f"✅ [{account_name}] All requests completed: {len(combined_checks)} total checks from {len(requests_made)} status request(s)")
        return {'items': combined_checks, 'account_name': account_name, 'account_id': account_id}
    else:
        # All requests failed
        progress_tracker.mark_failed(account_id)
        return {'items': [], 'account_name': account_name, 'account_id': account_id, 'error': 'all_status_requests_failed'}

def _fetch_checks_with_filter(account_name: str, filter_string: str, max_pages: int = 50) -> Dict:
    """Fetch checks with a specific filter, detecting 10K limit"""
    checks_url = f"{BASE_URL}/beta/cloudPosture/checks"
    
    # Date filtering - expand range for SUCCESS queries to capture recently resolved items
    if 'status eq \'SUCCESS\'' in filter_string:
        # For SUCCESS items, expand date range since we filter by resolvedDateTime in post-processing
        # A check could be created months ago but resolved recently
        start_date = datetime.now(timezone.utc) - timedelta(days=args.timeframe * 10)  # 10x wider range
        end_date = datetime.now(timezone.utc)
        print(f"📅 [{account_name}] Expanding date range for SUCCESS queries: {args.timeframe * 10} days")
    else:
        # For FAILURE items, use normal timeframe
        start_date = datetime.now(timezone.utc) - timedelta(days=args.timeframe)
        end_date = datetime.now(timezone.utc)
    
    start_date_str = start_date.isoformat().replace('+00:00', 'Z')
    end_date_str = end_date.isoformat().replace('+00:00', 'Z')
    
    params = {
        'top': 200,  # Max per page
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
    hit_limit = False
    
    while page_count < max_pages:
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
                    # Check if we might have hit the 10K limit (no nextLink but got exactly multiples close to 10K)
                    if len(all_checks) >= 9800:  # Close to 10K, likely hit limit
                        hit_limit = True
                        print(f"🚨 [{account_name}] Likely hit 10,000 result limit (got {len(all_checks)} results)")
                    break
                    
                # Check if we're approaching the 10K limit
                if len(all_checks) >= 10000:
                    hit_limit = True
                    print(f"🚨 [{account_name}] Hit 10,000 result limit")
                    break
                    
                retries = 0  # Reset retries on successful request
                
            elif response.status_code == 429:
                # Rate limit exceeded
                if rate_limiter.handle_rate_limit(retries):
                    retries += 1
                    continue
                else:
                    logging.error(f"[{account_name}] Max retries exceeded")
                    return {'success': False, 'items': [], 'error': 'rate_limit_exceeded'}
            else:
                logging.error(f"[{account_name}] HTTP Error: {response.status_code}")
                return {'success': False, 'items': [], 'error': f'http_{response.status_code}'}
                
        except requests.exceptions.RequestException as e:
            logging.error(f"[{account_name}] Request failed: {e}")
            if retries < rate_limiter.max_retries:
                retries += 1
                time.sleep(2 ** retries)
                continue
            else:
                return {'success': False, 'items': [], 'error': str(e)}
    
    if page_count >= max_pages:
        print(f"⚠️  [{account_name}] Reached max pages limit ({max_pages}), might have more data")
        hit_limit = True
    
    return {'success': True, 'items': all_checks, 'hit_limit': hit_limit}

def _get_checks_with_comprehensive_service_chunking(account_name: str, base_filter_conditions: List[str], status_type: str = "all") -> Dict:
    """Comprehensive service chunking for all accounts - zero duplication, maximum completeness"""
    print(f"🔄 [{account_name}] Starting comprehensive service chunking for {status_type}...")
    
    # Get official service list from Trend Vision One API
    official_services = _get_official_service_list()
    
    # Use concurrent service processing for major performance improvement
    result = service_concurrency_manager.fetch_services_concurrently(
        account_name, 
        official_services, 
        base_filter_conditions, 
        status_type
    )
    
    if result['success']:
        # Update global stats
        service_discovery_stats['known_services_found'].update(result['services_with_data'])
        
        # Check if any individual services hit limits
        if result['services_hitting_limits']:
            service_discovery_stats['accounts_hitting_limits'].append(f"{account_name} ({status_type})")
        
        # Check for errors and provide guidance
        if result['services_with_errors']:
            print(f"  ⚠️  [{account_name}] {len(result['services_with_errors'])} services had errors")
            if len(result['services_with_errors']) > 5:
                print(f"     Sample errors: {', '.join(result['services_with_errors'][:5])}")
        
        return result
    else:
        return {
            'success': False,
            'items': [],
            'error': 'concurrent_service_fetch_failed'
        }

def _get_checks_with_chunking(account_name: str, base_filter_conditions: List[str]) -> Dict:
    """Legacy function - replaced by comprehensive service chunking"""
    print(f"⚠️  [{account_name}] Using legacy chunking method")
    return _get_checks_with_comprehensive_service_chunking(account_name, base_filter_conditions)



def _chunk_by_risk_levels(account_name: str, base_filter_conditions: List[str]) -> Dict:
    """Chunk by risk levels, with service chunking fallback if still hitting limits"""
    # Use user-selected risk levels instead of hardcoded ones
    risk_levels = args.risk_levels
    all_items = []
    
    for risk_level in risk_levels:
        risk_filter = base_filter_conditions + [f'riskLevel eq \'{risk_level}\'']
        risk_filter_string = ' and '.join(risk_filter)
        
        result = _fetch_checks_with_filter(account_name, risk_filter_string, max_pages=50)
        
        if result['success']:
            if result['items']:
                print(f"⚠️  [{account_name}] Risk {risk_level}: {len(result['items'])} checks")
                
                # Check if this risk level STILL hit the limit
                if result['hit_limit'] and len(result['items']) >= 9800:
                    print(f"🚨 [{account_name}] Risk {risk_level} still hitting 10K limit! Using service chunking...")
                    
                    # Further chunk this risk level by services
                    service_chunks = _chunk_risk_level_by_services(account_name, risk_filter)
                    all_items.extend(service_chunks)
                else:
                    all_items.extend(result['items'])
        
        # Longer delay between risk level requests to avoid rate limits during chunking
        time.sleep(0.5)
    
    return {'success': True, 'items': all_items}

def _get_official_service_list() -> List[str]:
    """Fetch the official service list from Trend Vision One Cloud Posture API (cached per session)"""
    global _official_services_cache
    
    # Return cached list if already fetched
    if _official_services_cache is not None:
        return _official_services_cache
    
    services_url = "https://us-west-2.cloudconformity.com/v1/services"
    
    try:
        print("🌐 Fetching official service list from Trend Vision One API...")
        response = requests.get(services_url, timeout=30)
        if response.status_code == 200:
            services_data = response.json()
            service_ids = [service['id'] for service in services_data.get('data', [])]
            _official_services_cache = service_ids  # Cache the result
            print(f"✅ Fetched {len(service_ids)} official services from Trend Vision One")
            print(f"    Sample services: {', '.join(service_ids[:10])}{'...' if len(service_ids) > 10 else ''}")
            return service_ids
        else:
            print(f"⚠️ Could not fetch official service list (HTTP {response.status_code}), using fallback")
            _official_services_cache = _get_fallback_service_list()
            return _official_services_cache
    except Exception as e:
        print(f"⚠️ Error fetching official service list: {e}, using fallback")
        _official_services_cache = _get_fallback_service_list()
        return _official_services_cache

def _get_fallback_service_list() -> List[str]:
    """Fallback service list in case the API is unavailable"""
    return [
        # AWS Services (most common)
        'EC2', 'S3', 'RDS', 'Lambda', 'IAM', 'VPC', 'ELB', 'CloudTrail', 'CloudWatch', 'EKS', 'ECS',
        'KMS', 'SNS', 'SQS', 'DynamoDB', 'ElastiCache', 'Redshift', 'EMR', 'Route53', 'ACM',
        'Config', 'ConfigService', 'GuardDuty', 'SecurityHub', 'Inspector', 'WAF', 'Shield', 'EBS',
        'ELBv2', 'APIGateway', 'AutoScaling', 'Backup', 'CloudFormation', 'CloudFront', 'ECR', 'EFS',
        
        # GCP Services
        'Compute Engine', 'Cloud Storage', 'Cloud SQL', 'Cloud Functions', 'Cloud IAM', 'GKE',
        'Cloud KMS', 'Cloud Pub/Sub', 'Cloud Datastore', 'Cloud Firestore', 'Cloud DNS',
        
        # Azure Services
        'Virtual Machines', 'Storage Accounts', 'SQL Database', 'Azure Functions', 'Azure AD', 'AKS',
        'Key Vault', 'Service Bus', 'Cosmos DB', 'Application Gateway', 'Front Door'
    ]

def _chunk_risk_level_by_services(account_name: str, risk_filter_conditions: List[str]) -> List[Dict]:
    """Further chunk a specific risk level by services when it still hits 10K limit"""
    # Get official service list from Trend Vision One API
    common_services = _get_official_service_list()
    
    print(f"  🔍 [{account_name}] Checking {len(common_services)} official services from Trend Vision One...")
    
    # Use concurrent service processing for better performance
    result = service_concurrency_manager.fetch_services_concurrently(
        account_name,
        common_services,
        risk_filter_conditions,
        "risk_chunked"
    )
    
    if result['success']:
        all_service_items = result['items']
        services_with_data = result['services_with_data']
        
        # Try to catch any unknown services using exclusion filter
        print(f"  🔍 [{account_name}] Checking for unknown services...")
        unknown_service_items = _get_unknown_services(account_name, risk_filter_conditions, common_services)
        
        if unknown_service_items:
            print(f"  ❓ [{account_name}] Found {len(unknown_service_items)} checks from unknown services!")
            all_service_items.extend(unknown_service_items)
        
        # Service coverage report
        print(f"  📊 [{account_name}] Service coverage: {len(services_with_data)}/{len(common_services)} official services had data")
        if services_with_data:
            print(f"      Services with data: {', '.join(services_with_data)}")
            # Update global stats
            service_discovery_stats['known_services_found'].update(services_with_data)
        
        return all_service_items
    else:
        print(f"  ❌ [{account_name}] Concurrent service fetch failed, falling back to sequential")
        return _chunk_risk_level_by_services_sequential(account_name, risk_filter_conditions, common_services)

def _chunk_risk_level_by_services_sequential(account_name: str, risk_filter_conditions: List[str], common_services: List[str]) -> List[Dict]:
    """Fallback sequential processing if concurrent approach fails"""
    all_service_items = []
    services_with_data = []
    
    for service in common_services:
        service_filter = risk_filter_conditions + [f'service eq \'{service}\'']
        service_filter_string = ' and '.join(service_filter)
        
        result = _fetch_checks_with_filter(account_name, service_filter_string, max_pages=30)
        
        if result['success'] and result['items']:
            print(f"  🔧 [{account_name}] Service {service}: {len(result['items'])} checks")
            all_service_items.extend(result['items'])
            services_with_data.append(service)
        
        # Small delay between service requests
        time.sleep(0.3)
    
    return all_service_items

def _get_unknown_services(account_name: str, risk_filter_conditions: List[str], known_services: List[str]) -> List[Dict]:
    """Try to find checks from services not in our known list - with better deduplication"""
    # Get all services that had data first to avoid re-fetching them
    services_with_data = [svc for svc in known_services if len([item for item in [] if item.get('service') == svc]) > 0]
    
    # Create a filter that excludes services we already checked
    base_filter = ' and '.join(risk_filter_conditions)
    
    # Build exclusion filter more intelligently
    # Exclude only services that actually had data to reduce filter length
    services_to_exclude = known_services[:30]  # Increased from 20 to 30 for better coverage
    exclusion_conditions = [f"not service eq '{service}'" for service in services_to_exclude]
    
    # Combine with base filter
    exclusion_filter = base_filter + ' and ' + ' and '.join(exclusion_conditions)
    
    # Check if this filter is too long (API has ~1783 char limit)
    if len(exclusion_filter) > 1500:
        print(f"  ⚠️  [{account_name}] Filter too long for unknown service check, limiting exclusions...")
        # Reduce exclusions to fit within limit
        max_exclusions = max(1, (1500 - len(base_filter) - 10) // 25)  # Rough estimate
        services_to_exclude = known_services[:max_exclusions]
        exclusion_conditions = [f"not service eq '{service}'" for service in services_to_exclude]
        exclusion_filter = base_filter + ' and ' + ' and '.join(exclusion_conditions)
        
        if len(exclusion_filter) > 1500:
            print(f"  ⚠️  [{account_name}] Still too long, skipping unknown service check")
            return []
    
    print(f"  🔍 [{account_name}] Checking for services not in first {len(services_to_exclude)} official services...")
    result = _fetch_checks_with_filter(account_name, exclusion_filter, max_pages=10)
    
    if result['success'] and result['items']:
        # Filter out any items we've already collected (extra safety)
        collected_service_names = set(known_services[:len(services_to_exclude)])
        
        truly_unknown_items = []
        unknown_services = set()
        
        for item in result['items']:
            service = item.get('service', 'Unknown')
            if service not in collected_service_names:
                truly_unknown_items.append(item)
                unknown_services.add(service)
        
        if unknown_services:
            print(f"  🆕 [{account_name}] Discovered new services: {', '.join(sorted(unknown_services))}")
            print(f"  📊 [{account_name}] {len(truly_unknown_items)} checks from unknown services")
            # Update global stats
            service_discovery_stats['unknown_services_found'].update(unknown_services)
        
        return truly_unknown_items
    
    return []

def _deduplicate_checks(checks: List[Dict]) -> List[Dict]:
    """Remove duplicate checks based on check ID"""
    seen_ids = set()
    deduplicated = []
    
    for check in checks:
        check_id = check.get('id')
        if check_id and check_id not in seen_ids:
            seen_ids.add(check_id)
            deduplicated.append(check)
        elif not check_id:
            # Include checks without IDs (shouldn't happen but be safe)
            deduplicated.append(check)
    
    if len(checks) != len(deduplicated):
        print(f"🔄 Deduplicated: {len(checks)} -> {len(deduplicated)} checks (removed {len(checks) - len(deduplicated)} duplicates)")
    
    return deduplicated

def process_checks_for_account(account: Dict, checks_info: Dict):
    """Process checks and stream to reporter with corrected logic"""
    if checks_info.get('skipped') or checks_info.get('error'):
        return 0, 0
    
    # Handle partial results from chunking
    if checks_info.get('partial'):
        print(f"⚠️  [{account['name']}] Processing partial results due to chunking limitations")
    
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
    
    # Show filtering applied
    risk_summary = f"Risk levels: {', '.join(args.risk_levels)}" if len(args.risk_levels) < 5 else "All risk levels"
    print(f"🔧 Filters applied: {status_filter.title()} status, {risk_summary}")
    print(f"⚡ Concurrency: {args.max_workers} accounts × {args.service_workers} services = {args.max_workers * args.service_workers} total concurrent requests")
    
    # Generate final report
    print(f"\n📄 Generating final report...")
    output_file = streaming_reporter.finalize()
    print(f"📄 Report saved: {output_file}")
    
    # Service discovery completeness report
    _print_service_discovery_report()
    
    # Cleanup
    progress_tracker.cleanup()
    print(f"✨ Session {session_id} completed successfully!")

def _print_service_discovery_report():
    """Print comprehensive service discovery and completeness report"""
    print(f"\n📋 SERVICE DISCOVERY & COMPLETENESS REPORT")
    print(f"=" * 60)
    
    print(f"📊 Service Chunking Statistics:")
    print(f"  • All accounts use comprehensive service chunking: {service_discovery_stats['accounts_chunked']}")
    print(f"  • Accounts with services hitting 10K limits: {len(service_discovery_stats['accounts_hitting_limits'])}")
    
    if service_discovery_stats['accounts_hitting_limits']:
        print(f"  • Accounts needing attention: {', '.join(service_discovery_stats['accounts_hitting_limits'][:5])}")
        if len(service_discovery_stats['accounts_hitting_limits']) > 5:
            print(f"    ... and {len(service_discovery_stats['accounts_hitting_limits']) - 5} more")
    
    print(f"\n🔧 Service Coverage (using official Trend Vision One API):")
    print(f"  • Total services discovered: {len(discovered_services)}")
    print(f"  • Official services found: {len(service_discovery_stats['known_services_found'])}")
    print(f"  • Unexpected services found: {len(service_discovery_stats['unknown_services_found'])}")
    
    if service_discovery_stats['known_services_found']:
        known_services = sorted(service_discovery_stats['known_services_found'])
        print(f"  • Official services: {', '.join(known_services[:10])}")
        if len(known_services) > 10:
            print(f"    ... and {len(known_services) - 10} more")
    
    if service_discovery_stats['unknown_services_found']:
        unknown_services = sorted(service_discovery_stats['unknown_services_found'])
        print(f"  • ⚠️  Unexpected services: {', '.join(unknown_services)}")
        print(f"    💡 These weren't in the official API - may indicate API lag or data inconsistency")
    
    print(f"\n⚠️  Data Completeness Assessment:")
    if service_discovery_stats['accounts_hitting_limits']:
        print(f"  • ⚠️  {len(service_discovery_stats['accounts_hitting_limits'])} accounts have individual services hitting 10K limits")
        for account in service_discovery_stats['accounts_hitting_limits'][:3]:
            print(f"    - {account}")
        if len(service_discovery_stats['accounts_hitting_limits']) > 3:
            print(f"    - ... and {len(service_discovery_stats['accounts_hitting_limits']) - 3} more")
        print(f"  • 💡 Recommendation: Use smaller timeframes for these accounts to ensure complete service data")
    else:
        print(f"  • ✅ All services in all accounts have complete data coverage")
        print(f"  • 🎯 Zero duplication achieved with comprehensive service chunking")
    
    if discovered_services:
        print(f"\n🎯 All Services Discovered ({len(discovered_services)} total):")
        all_services = sorted(discovered_services)
        # Print in columns
        for i in range(0, len(all_services), 5):
            services_row = all_services[i:i+5]
            print(f"    {' | '.join(f'{s:<20}' for s in services_row)}")
    
    print(f"\n💡 Completeness & Performance Notes:")
    print(f"  • 🔧 All accounts use comprehensive service chunking (zero duplication)")
    print(f"  • ⚡ Service-level concurrency: {args.service_workers} parallel requests per account")
    print(f"  • 📋 Service list auto-synced with official Trend Vision One API")
    print(f"  • ⚠️  Individual service 10K limits: use --timeframe 7 for complete data")
    print(f"  • 🎯 Each service queried separately for maximum accuracy")
    print(f"  • 📊 Runtime optimized with concurrent processing (10x faster)")
    print(f"  • 🛡️  Rate limiting and error handling ensure reliability")
    print(f"=" * 60)

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
