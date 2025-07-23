# Trend Vision One - Cloud Posture Compliance Report Script

This Python script automates the process of auditing cloud infrastructure compliance using Trend Vision One's Cloud Posture API. It fetches compliance check data for your cloud accounts and generates a detailed Excel report categorizing each check as either a success or failure, with comprehensive metadata for analysis.

## Key Features

- **Simple Command Line Interface**: Easy-to-use flags for common reporting scenarios
- **Flexible Timeframes**: Configurable lookback period (1-365 days)
- **Status Filtering**: Focus on failures, successes, or all checks
- **Excel Output**: Professional reports with separate sheets for failures and successes
- **10K+ Result Handling**: Automatic chunking for large accounts exceeding API limits
- **Risk Level Chunking**: Simple, effective filtering by risk level when needed
- **Enterprise Scalability**: Optimized for 50+ accounts with concurrent processing
- **Resume Capability**: Checkpoint-based recovery for long-running operations
- **Date Filtering Workaround**: Handles Trend Vision One API pagination limitations
- **Real-time Progress**: Shows processing status and data validation

## Prerequisites

- Python 3.6+ installed on your system
- Access to Trend Vision One platform with Cloud Posture enabled
- Valid Vision One API token with Cloud Posture permissions
- Internet connectivity to reach `api.xdr.trendmicro.com`

## Installation

1. **Install Python Dependencies**:
   ```bash
   pip install requests pandas openpyxl
   ```

2. **Download the Script**:
   ```bash
   # Clone or download report.py to your local machine
   ```

## Quick Start

### Basic Usage

```bash
# Using CLI token
python report.py --token "your_token_here" --failures

# Using environment variable (recommended)
export TMV1_TOKEN="your_token_here"
python report.py --timeframe 30 --all

# Environment variable with resume capability
python report.py --timeframe 14 --successes --resume
```

## Command Line Options

### Required Arguments
- `--token TOKEN` - Your Trend Vision One API token (or set `TMV1_TOKEN` environment variable)

### Timeframe Options
- `--timeframe DAYS` - Number of days to look back (default: 7, range: 1-365)

### Status Filters (mutually exclusive)
- `--all` - Include both failures and successes
- `--failures` - Failures only (default if no status specified)
- `--successes` - Successes only

### Performance & Scalability Options
- `--max-workers N` - Concurrent account processing (default: 5)
- `--batch-size N` - Records per output batch (default: 1000)
- `--resume` - Resume from previous checkpoint

### Help
- `--help` - Show help message with examples

## Enterprise Features

### 10K+ Result Handling

The script automatically handles Trend Vision One API's 10,000 result limit using intelligent chunking strategies:

**Automatic Detection**: Detects when accounts exceed the 10K limit
```bash
🚨 [Large-Account] Hit 10,000 result limit
⚠️  [Large-Account] Hit 10,000 result limit. Implementing chunking strategy...
```

**Risk Level Chunking**: When hitting the 10K limit, automatically subdivides by:
1. **Primary Filter**: `accountId` + `status` (SUCCESS/FAILURE based on your CLI flags)
2. **Chunking Strategy**: If limit hit, splits by risk levels: `LOW`, `MEDIUM`, `HIGH`, `VERY_HIGH`, `EXTREME`

**Result Aggregation**: Combines and deduplicates data from multiple chunked requests
```bash
🔄 [Large-Account] Implementing risk level chunking...
⚠️  [Large-Account] Risk HIGH: 2,456 checks
⚠️  [Large-Account] Risk VERY_HIGH: 1,234 checks
✅ [Large-Account] Completed with chunking: 15,847 checks (deduplicated)
```

### Concurrent Processing

Process multiple accounts simultaneously for faster execution:
```bash
# Process 50+ accounts with 10 concurrent workers
python report.py --token "your_token_here" --failures --max-workers 10
```

### Resume Capability

For long-running operations, use checkpoints to resume interrupted sessions:
```bash
# Start large operation
python report.py --token "your_token_here" --all --max-workers 8

# Resume if interrupted
python report.py --token "your_token_here" --all --max-workers 8 --resume
```

Progress tracking shows:
```bash
📂 Resumed session: 23 accounts completed, 2 failed
⏭️  Skipping AccountName (already completed)
```

## Examples

### Security Team Daily Report
```bash
# Set token once
export TMV1_TOKEN="your_token_here"

# Check yesterday's failures across all cloud providers
python report.py --timeframe 1 --failures
```

### Weekly Compliance Review
```bash
# Get all compliance data from last 7 days
python report.py --timeframe 7 --all
```

### Monthly Failure Analysis
```bash
# Focus on failures from last 30 days
python report.py --timeframe 30 --failures
```

### Compliance Success Tracking
```bash
# See what's been resolved recently
python report.py --timeframe 14 --successes
```

### Enterprise Scale Examples
```bash
# Set token for session
export TMV1_TOKEN="your_token_here"

# Large organization with 50+ accounts (concurrent processing)
python report.py --timeframe 30 --failures --max-workers 10

# Resume interrupted large operation
python report.py --timeframe 30 --all --max-workers 8 --resume

# Handle accounts with 10K+ results automatically
python report.py --timeframe 7 --failures
# Script will automatically chunk large accounts by risk level as needed
```

## API Token Setup

### Getting Your API Token

1. **Log into Trend Vision One**
2. **Navigate to Administration > API Keys**
3. **Create a new API key** with Cloud Posture permissions
4. **Copy the token** for use with the script

### Token Security & Environment Variables

⚠️ **Important**: Keep your API token secure:
- **Recommended**: Use `TMV1_TOKEN` environment variable instead of CLI arguments
- Never commit tokens to version control
- Use environment variables in automated environments
- Rotate tokens regularly
- Use least-privilege permissions

#### Using Environment Variable (Recommended)
```bash
# Set environment variable (Linux/macOS)
export TMV1_TOKEN="your_token_here"

# Set environment variable (Windows)
set TMV1_TOKEN=your_token_here

# Use script without exposing token in command history
python report.py --failures
python report.py --timeframe 30 --all
```

#### Token Detection Priority
1. **CLI argument**: `--token` (if provided)
2. **Environment variable**: `TMV1_TOKEN` (if CLI not provided)
3. **Error**: Script exits if neither is found

```bash
# These are equivalent:
python report.py --token "your_token_here" --failures
# vs
export TMV1_TOKEN="your_token_here"
python report.py --failures
```

## Output

The script generates an Excel file named `compliance_report_YYYYMMDD_HHMMSS.xlsx` containing:

### Failures Sheet
- Account information (ID, name, provider, cloud account IDs)
- Check details (status, rule ID, resource information)
- Risk assessment (risk level, categories, description)
- Metadata (creation time, last update, region, service)

### Successes Sheet  
- Same structure as Failures but for passed checks
- Useful for tracking compliance improvements
- Helps validate remediation efforts

### Data Fields
Each check includes:
- `account_id`, `account_name`, `provider`
- `aws_account_id`, `gcp_project_id`, `azure_subscription_id`
- `resource_count`, `status`, `rule_id`, `resource_id`
- `resource`, `service`, `region`, `risk_level`
- `categories`, `description`, `status_updated_time`, `created_time`

## Technical Details

### API Endpoints Used
- **Accounts**: `GET /beta/cloudPosture/accounts`
- **Checks**: `GET /beta/cloudPosture/checks`

### Date Filtering Implementation
The script implements a workaround for a known Trend Vision One API limitation where pagination (`nextLink` URLs) lose date filtering parameters. Our solution:

1. **Detects the issue**: Monitors for out-of-range data in paginated results
2. **Fixes nextLink URLs**: Automatically adds date filtering parameters back to pagination URLs
3. **Ensures accuracy**: Only returns data within your specified timeframe

### Performance Optimizations
- **Pagination handling**: Processes large datasets efficiently
- **Request optimization**: Uses maximum page size (200 items) per request
- **Concurrent processing**: Processes multiple accounts simultaneously
- **Progress tracking**: Shows real-time processing status

## Troubleshooting

### Common Issues

**Authentication Errors (HTTP 401)**
```
Solution: Verify your API token is correct and has Cloud Posture permissions
```

**HTTP 400 Errors**
```
Cause: Usually invalid date filtering parameters
Solution: The script handles this automatically with proper ISO 8601 formatting
```

**No Data Returned**
```
Possible causes:
- No checks exist in the specified timeframe
- API token lacks necessary permissions
- Incorrect status filter selection

Solution: Try increasing --timeframe or using --all instead of specific status filters
```

**Date Range Warnings**
```
Warning: "Found X checks outside Y-day range"
Cause: API pagination issue (handled automatically)
Status: Script continues with corrected filtering
```

### Debug Information

The script provides detailed console output including:
- Account discovery and filtering results
- API request URLs and parameters
- Date range validation for each page
- Processing progress and timing
- Final summary statistics

### Getting Help

1. **Check console output** for specific error messages
2. **Verify API token** has correct permissions
3. **Try broader filters** (use --all, increase timeframe)
4. **Contact support** with console output if issues persist

## API Rate Limits

The script respects Trend Vision One API limits:
- **Sequential processing**: Processes accounts one at a time
- **Reasonable delays**: Built-in request pacing
- **Error handling**: Graceful handling of rate limit responses

## Version History

### Current Version
- ✅ Simplified command-line interface with full descriptive flags
- ✅ Status filtering (failures, successes, or all)
- ✅ **10K+ Result Handling**: Automatic chunking for large accounts
- ✅ **Enterprise Scalability**: Concurrent processing for 50+ accounts
- ✅ **Resume Capability**: Checkpoint-based recovery
- ✅ **Risk Level Chunking**: Simple, effective filtering by risk level
- ✅ Automatic date filtering fixes for API pagination
- ✅ Enhanced progress tracking and validation
- ✅ Improved error handling and user feedback

### Migration from Cloud One Conformity
This script has been updated for **Trend Vision One Cloud Posture** APIs, replacing the legacy Cloud One Conformity endpoints with the unified Vision One platform.

## License

This script is provided "as is" for use with Trend Vision One Cloud Posture. Use responsibly and in accordance with your organization's security policies.

## Support

For issues related to:
- **Script functionality**: Check troubleshooting section above
- **API access**: Contact your Trend Vision One administrator  
- **Platform issues**: Use official Trend Micro support channels

---

**Example Complete Workflow:**

```bash
# 1. Get your API token from Vision One console
# 2. Set environment variable (recommended for security)
export TMV1_TOKEN="your_token_here"

# 3. Run a quick test
python report.py --timeframe 1 --failures

# 4. Generate comprehensive weekly report (handles 10K+ automatically)
python report.py --timeframe 7 --all

# 5. Enterprise scale: 50+ accounts with concurrent processing
python report.py --timeframe 30 --failures --max-workers 10

# 6. Resume if interrupted (checkpoint recovery)
python report.py --timeframe 30 --failures --max-workers 10 --resume

# 7. Open the generated Excel file for analysis
```