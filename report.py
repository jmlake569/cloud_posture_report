import os
import sys
import csv
import json
import argparse
import time
import random
from datetime import datetime, timedelta, timezone

import requests

# === AUTH ===
API_TOKEN = os.getenv('TMV1_TOKEN')
if not API_TOKEN:
    print("❌ ERROR: Missing required environment variable 'TMV1_TOKEN'.")
    sys.exit(1)

# === API ===
API_BASE = 'https://api.xdr.trendmicro.com'
ACCOUNTS_ENDPOINT = '/beta/cloudPosture/accounts'
CHECKS_ENDPOINT = '/beta/cloudPosture/checks'

HEADERS = {
    'Authorization': f'Bearer {API_TOKEN}',
    'Content-Type': 'application/json',
}

# === ADAPTIVE PAGING ===
MAX_PAGE_SIZE = 200
MIN_PAGE_SIZE = 50
RETRY_MAX = 3

# Region seeds (comprehensive lists to avoid 10K limit)
AWS_COMMON = [
    'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
    'eu-west-1', 'eu-west-2', 'eu-central-1', 'eu-north-1',
    'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1', 'ap-northeast-2',
    'sa-east-1', 'ca-central-1', 'af-south-1', 'me-south-1',
    'global'  # Some services are global
]
AZURE_COMMON = [
    'eastus', 'eastus2', 'westus', 'westus2', 'westus3',
    'westeurope', 'northeurope', 'southeastasia', 'eastasia',
    'centralus', 'southcentralus', 'northcentralus',
    'canadacentral', 'canadaeast', 'brazilsouth', 'australiaeast',
    'australiasoutheast', 'japaneast', 'japanwest', 'koreacentral',
    'global'  # Some services are global
]
GCP_COMMON = [
    'us-central1', 'us-east1', 'us-east4', 'us-west1', 'us-west2', 'us-west3', 'us-west4',
    'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4', 'europe-west6',
    'asia-southeast1', 'asia-southeast2', 'asia-northeast1', 'asia-northeast2',
    'australia-southeast1', 'southamerica-east1', 'northamerica-northeast1',
    'global'  # Some services are global
]

def parse_args():
    p = argparse.ArgumentParser(
        description="Export Trend Micro Cloud Posture checks to CSV (always partitions by region)."
    )
    p.add_argument('--days', type=int, default=30, help='Days back from now to look for resolved issues (default: 30).')
    p.add_argument('--top', type=int, default=200, help='Page size 50–200 (default: 200).')
    p.add_argument('--outfile', default='cloud_posture_checks.csv', help='CSV output file.')
    p.add_argument('--debug', action='store_true', help='Print raw API payloads / extra logs.')
    return p.parse_args()

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def fetch_accounts(debug=False):
    print("🔍 Fetching cloud accounts...")
    try:
        resp = requests.get(API_BASE + ACCOUNTS_ENDPOINT, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if debug:
            print(f"Raw accounts payload:\n{json.dumps(data, indent=2)[:2000]}\n...")
        items = data.get('items', [])
        print(f"✔️ Found {len(items)} accounts.")
        return items
    except requests.exceptions.HTTPError:
        print(f"❌ HTTP Error fetching accounts: {resp.status_code} - {resp.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error fetching accounts: {e}")
    except ValueError:
        print("❌ JSON decode error fetching accounts.")
    return []

def build_time_params(start_iso: str, end_iso: str) -> dict:
    return {
        'startDateTime': start_iso,
        'endDateTime': end_iso,
        'dateTimeTarget': 'createdDate',  # API only supports createdDate filtering
    }

def _clamp_top(top: int) -> int:
    return max(MIN_PAGE_SIZE, min(top, MAX_PAGE_SIZE))

# === SERVER-SIDE FILTER HELPERS ===
def _risk_filter(use_plural: bool) -> str:
    """
    Build ONLY the OR group for risk levels.
    No ANDs inside parentheses to satisfy API rule.
    """
    # API only supports 'riskLevel', not 'riskLevels'
    return "(riskLevel eq 'HIGH' or riskLevel eq 'VERY_HIGH' or riskLevel eq 'EXTREME')"

def _compose_filter(account_id: str | None, extra_filter: str | None, use_plural: bool = False) -> str:
    """
    Compose TMV1-Filter without placing AND inside any parentheses group.
    Final shape:
      (accountId eq '...') and (extra) and (risk OR risk OR risk) and status eq 'SUCCESS'
    """
    parts = []
    if account_id:
        parts.append(f"(accountId eq '{account_id}')")
    if extra_filter:
        parts.append(f"({extra_filter})")
    parts.append(_risk_filter(use_plural))          # only ORs inside
    parts.append("status eq 'SUCCESS'")             # AND is outside - looking for SUCCESS checks that were resolved
    return " and ".join(parts)

def query_checks(account_id: str, params: dict, extra_filter: str = None,
                 top: int = 100, skip: int = 0, timeout: int = 30):
    """
    One-page request with:
      - server-side filters (accountId, optional extra, risk + status)
      - detailed error logging
      - adaptive backoff on 429/5xx/timeouts/JSON
      - one-time fallback retry on 400 swapping riskLevel -> riskLevels
    Returns (json, error_str).
    """
    def _do_request(use_plural: bool, current_top: int):
        hdrs = HEADERS.copy()
        tmv1 = _compose_filter(account_id, extra_filter, use_plural)
        hdrs['TMV1-Filter'] = tmv1

        q = params.copy()
        q['top'] = current_top
        q['skip'] = skip

        # inner adaptive loop
        attempt = 0
        while attempt < RETRY_MAX:
            try:
                resp = requests.get(API_BASE + CHECKS_ENDPOINT, headers=hdrs, params=q, timeout=timeout)
                # backoff statuses
                if resp.status_code in (429, 502, 503, 504):
                    attempt += 1
                    next_top = max(MIN_PAGE_SIZE, current_top // 2)
                    sleep_s = (2 ** attempt) + random.uniform(0, 0.5)
                    print(f"   ⚠️ {resp.status_code} from API, retry {attempt}/{RETRY_MAX} after {sleep_s:.1f}s with top={next_top}")
                    time.sleep(sleep_s)
                    current_top = next_top
                    continue

                resp.raise_for_status()
                return resp.json(), None

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 'HTTPError'
                body = e.response.text[:800] if e.response is not None else ''
                print(f"   ❌ HTTP {status} — TMV1-Filter: {tmv1}")
                print(f"      Body: {body}")
                return None, f"HTTP {status}: {body[:200]}"

            except requests.exceptions.Timeout:
                attempt += 1
                next_top = max(MIN_PAGE_SIZE, current_top // 2)
                sleep_s = (2 ** attempt) + random.uniform(0, 0.5)
                print(f"   ⚠️ Timeout, retry {attempt}/{RETRY_MAX} after {sleep_s:.1f}s with top={next_top}")
                time.sleep(sleep_s)
                current_top = next_top

            except requests.exceptions.RequestException as e:
                return None, f"Network error: {e}"

            except ValueError:
                # JSON decode error — try once with smaller page
                attempt += 1
                next_top = max(MIN_PAGE_SIZE, current_top // 2)
                print(f"   ⚠️ JSON decode error, retry {attempt}/{RETRY_MAX} with top={next_top}")
                time.sleep(0.5)
                current_top = next_top

        return None, f"Failed after {RETRY_MAX} attempts (last top={current_top})"

    # API only supports 'riskLevel', not 'riskLevels'
    current_top = _clamp_top(top)
    return _do_request(use_plural=False, current_top=current_top)

def get_total_count(account_id: str, start_iso: str, end_iso: str, top: int, debug=False):
    """Probe first page (risk-filtered) to read `count` and sample regions."""
    params = build_time_params(start_iso, end_iso)
    js, err = query_checks(account_id, params, extra_filter=None, top=top, skip=0)
    if err:
        print(f"   ❌ Count probe failed for {account_id}: {err}")
        return 0, set()
    count = js.get('count') or 0
    items = js.get('items') or js.get('checks') or []
    sample_regions = {it.get('region') for it in items if it.get('region')}
    if debug:
        print(f"   [DEBUG] count={count}, sample_regions={sorted(sample_regions)}")
    return count, sample_regions

def probe_regions_by_provider(account_provider: str):
    if account_provider == 'aws':
        return AWS_COMMON
    if account_provider == 'azure':
        return AZURE_COMMON
    if account_provider == 'gcp':
        return GCP_COMMON
    return []  # unknown provider; skip extra probes

def region_has_data(account_id: str, start_iso: str, end_iso: str, region: str, top: int, debug=False):
    """Probe a region (with risk filter) cheaply to see if any results exist."""
    params = build_time_params(start_iso, end_iso)
    extra = f"region eq '{region}'"
    js, err = query_checks(account_id, params, extra_filter=extra, top=MIN_PAGE_SIZE, skip=0)
    if err:
        if debug:
            print(f"   [DEBUG] region probe {region} error: {err}")
        return False
    items = js.get('items') or js.get('checks') or []
    has_data = len(items) > 0
    if debug and has_data:
        print(f"   [DEBUG] region {region} has {len(items)} items")
    return has_data

def fetch_checks_partitioned_by_region(account_id: str, start_iso: str, end_iso: str, top: int,
                                      provider: str, debug=False):
    """
    Partition retrieval by region to avoid 10K limit:
      1) Always probe common regions for the provider
      2) Fetch each region separately with pagination
      3) Use TMV1-Filter: accountId AND region AND riskLevel AND status
    """
    params = build_time_params(start_iso, end_iso)
    
    # Always probe common regions to ensure we don't miss any
    common_regions = probe_regions_by_provider(provider)
    if not common_regions:
        print("   ⚠️  Unknown provider, using basic regions.")
        common_regions = ['us-east-1', 'us-west-2', 'eu-west-1']  # fallback
    
    print(f"   🔍 Probing {len(common_regions)} regions for data...")
    discovered_regions = set()
    
    # Probe each common region to see if it has data
    for region in common_regions:
        if region_has_data(account_id, start_iso, end_iso, region, top, debug=debug):
            discovered_regions.add(region)
            if debug:
                print(f"   [DEBUG] Region {region} has data")
    
    print(f"   ✅ Found data in {len(discovered_regions)} regions")
    
    if not discovered_regions:
        print("   ⚠️  No regions discovered; falling back to non-partitioned fetch.")
        return fetch_checks_unpartitioned(account_id, start_iso, end_iso, top, debug=debug)

    # Fetch each discovered region with full pagination
    all_items = []
    for region in sorted(discovered_regions):
        print(f"   → Region: {region}")
        skip = 0
        pages = 0
        total_region = 0
        
        while True:
            extra = f"region eq '{region}'"
            js, err = query_checks(account_id, params, extra_filter=extra, top=top, skip=skip)
            if err:
                print(f"      ❌ Error region {region}: {err}")
                break
                
            items = js.get('items') or js.get('checks') or []
            all_items.extend(items)
            got = len(items)
            total_region += got
            pages += 1
            
            if debug:
                print(f"      [DEBUG] region {region}: page {pages}, got={got}, skip={skip}")

            count = js.get('count')
            if count is not None:
                if skip + got >= count or got == 0:
                    break
            else:
                if got == 0:
                    break
            skip += got
            
        print(f"      ↳ Region {region}: {total_region} items")
        
    print(f"   ↳ Retrieved {len(all_items)} checks (partitioned by {len(discovered_regions)} region(s)).")
    return all_items

def fetch_checks_unpartitioned(account_id: str, start_iso: str, end_iso: str, top: int, debug=False):
    """Fallback only (kept for completeness); still applies risk filter server-side."""
    print(f"→ Fetching checks (unpartitioned) for account: {account_id}")
    params = build_time_params(start_iso, end_iso)
    all_items, skip = [], 0
    while True:
        js, err = query_checks(account_id, params, extra_filter=None, top=top, skip=skip)
        if err:
            print(f"   ❌ Error: {err}")
            break
        items = js.get('items') or js.get('checks') or []
        all_items.extend(items)
        got = len(items)
        count = js.get('count')
        if count is not None:
            if skip + got >= count or got == 0:
                break
        else:
            if got == 0:
                break
        skip += got
    print(f"   ↳ Retrieved {len(all_items)} checks.")
    return all_items

# === FULL-FIELD CSV EXPORT ===
def flatten_value(val):
    """Flatten lists/dicts into JSON strings for CSV compatibility."""
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    return val

def export_to_csv_full(records, filename):
    """Export with dynamic headers including all keys present in any record."""
    if not records:
        print("⚠️ No data to export.")
        return

    # Gather all unique keys across all records
    all_keys = set()
    for rec in records:
        all_keys.update(rec.keys())
    headers = sorted(all_keys)  # deterministic order

    print(f"\n📝 Writing {len(records)} checks with {len(headers)} columns to CSV: {filename}")
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for rec in records:
                writer.writerow({k: flatten_value(rec.get(k, "")) for k in headers})
        print("✅ Export complete.\n")
    except Exception as e:
        print(f"❌ Failed to write CSV: {e}")

# === CLIENT-SIDE FILTER: ONLY KEEP CHECKS WITH RESOLVED DATE ===
def has_resolved(chk: dict) -> bool:
    """True if resolvedDateTime exists and is non-empty."""
    val = chk.get('resolvedDateTime')
    if val is None:
        return False
    if isinstance(val, str):
        return val.strip() != ""
    return True  # non-string truthy value

def was_resolved_in_timeframe(chk: dict, start_dt: datetime, end_dt: datetime) -> bool:
    """True if the check was resolved within the specified timeframe."""
    resolved_str = chk.get('resolvedDateTime')
    if not resolved_str:
        return False
    
    try:
        # Parse the resolved date
        resolved_dt = datetime.fromisoformat(resolved_str.replace('Z', '+00:00'))
        return start_dt <= resolved_dt <= end_dt
    except (ValueError, TypeError):
        return False

def debug_check_fields(chk: dict, debug: bool = False):
    """Debug helper to show all fields that might contain resolution info."""
    if not debug:
        return
    
    print(f"   [DEBUG] All check fields: {sorted(chk.keys())}")
    
    # Look for any field that might contain resolution info
    resolution_fields = ['resolvedDateTime', 'resolvedDate', 'resolutionDate', 'fixedDate', 'closedDate', 'updatedDateTime']
    for field in resolution_fields:
        val = chk.get(field)
        if val is not None:
            print(f"   [DEBUG] {field}: {val}")
    
    # Show key fields
    print(f"   [DEBUG] status: {chk.get('status')}")
    print(f"   [DEBUG] riskLevel: {chk.get('riskLevel')}")
    print(f"   [DEBUG] createdDateTime: {chk.get('createdDateTime')}")
    print(f"   [DEBUG] updatedDateTime: {chk.get('updatedDateTime')}")
    print("   [DEBUG] ---")

def main():
    args = parse_args()

    # normalize
    if args.top < MIN_PAGE_SIZE or args.top > MAX_PAGE_SIZE:
        print(f"⚠️  --top must be between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}. Using 200.")
        args.top = 200
    if args.days <= 0:
        print("⚠️  --days must be positive. Using 30.")
        args.days = 30

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=args.days)
    start_iso, end_iso = iso_utc(start_dt), iso_utc(end_dt)
    
    # For resolved issues, we need to fetch a broader range since we can't filter by resolvedDate on server
    # Fetch 3x the timeframe to catch issues that were created earlier but resolved recently
    fetch_start_dt = end_dt - timedelta(days=args.days * 3)
    fetch_start_iso = iso_utc(fetch_start_dt)

    accounts = fetch_accounts(debug=args.debug)
    if not accounts:
        print("⚠️ No accounts retrieved. Exiting.")
        return

    all_rows = []
    total_checks = 0
    resolved_checks = 0
    for acc in accounts:
        acc_id = acc.get('id')
        acc_name = acc.get('name', 'Unknown')
        provider = (acc.get('provider') or '').lower()

        print(f"⚠️  Always partitioning account {acc_name} by region (risk filter: HIGH/VERY_HIGH/EXTREME + SUCCESS, looking for resolved issues).")
        checks = fetch_checks_partitioned_by_region(
            account_id=acc_id,
            start_iso=fetch_start_iso,  # Use broader fetch range
            end_iso=end_iso,
            top=args.top,
            provider=provider,
            debug=args.debug
        )

        total_checks += len(checks)
        
        # Debug: Show sample check structure
        if args.debug and len(checks) > 0:
            print(f"   [DEBUG] Sample check from {acc_name}:")
            debug_check_fields(checks[0], debug=True)
        
        for chk in checks:
            # Only include checks that were resolved within the target timeframe
            if not was_resolved_in_timeframe(chk, start_dt, end_dt):
                continue

            resolved_checks += 1
            row = dict(chk)  # shallow copy
            row.setdefault('accountId', acc_id)
            row.setdefault('accountName', acc_name)
            all_rows.append(row)
        
        # Debug: Check if we're getting region diversity
        if args.debug and len(checks) > 0:
            regions_in_data = {chk.get('region', 'unknown') for chk in checks}
            print(f"   [DEBUG] Regions in data: {sorted(regions_in_data)}")
            if len(regions_in_data) == 1 and 'global' in regions_in_data:
                print(f"   [WARNING] All data is global - region partitioning may not be working")
        
        # TEMPORARY: Let's also collect some non-resolved checks for debugging
        if args.debug and len(checks) > 0:
            non_resolved_sample = [chk for chk in checks if not has_resolved(chk)][:3]
            if non_resolved_sample:
                print(f"   [DEBUG] Sample non-resolved checks from {acc_name}:")
                for i, chk in enumerate(non_resolved_sample):
                    print(f"   [DEBUG] Non-resolved check {i+1}:")
                    debug_check_fields(chk, debug=True)

    print(f"\n📊 Summary: Retrieved {total_checks} total SUCCESS checks, {len(all_rows)} had resolution dates")
    
    if all_rows:
        export_to_csv_full(all_rows, args.outfile)
    else:
        print("⚠️ No checks found to export.")

if __name__ == '__main__':
    main()
