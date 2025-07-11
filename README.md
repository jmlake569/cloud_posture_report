# Trend Vision One - Cloud Posture Compliance Report Script

This Python script automates the process of auditing cloud infrastructure compliance using Trend Vision One's Cloud Posture API. It fetches compliance check data for your cloud accounts and generates a detailed Excel report categorizing each check as either a success or failure, with comprehensive metadata for analysis.

## Key Features

- **Simple Command Line Interface**: Easy-to-use flags for common reporting scenarios
- **Provider Filtering**: Target specific cloud providers (AWS, Azure, GCP)
- **Flexible Timeframes**: Configurable lookback period (1-365 days)
- **Status Filtering**: Focus on failures, successes, or all checks
- **Excel Output**: Professional reports with separate sheets for failures and successes
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
# Get failures from last 7 days (all providers)
python report.py --token "your_token_here" --f

# Get all checks from last 30 days for AWS only
python report.py --token "your_token_here" --timeframe 30 --all --aws

# Get successes from last 14 days for Azure
python report.py --token "your_token_here" --timeframe 14 --s --azure
```

## Command Line Options

### Required Arguments
- `--token TOKEN` - Your Trend Vision One API token (required)

### Timeframe Options
- `--timeframe DAYS` - Number of days to look back (default: 7, range: 1-365)

### Status Filters (mutually exclusive)
- `--all` - Include both failures and successes
- `--f` - Failures only (default if no status specified)
- `--s` - Successes only

### Provider Filters (mutually exclusive)
- `--aws` - AWS accounts only
- `--azure` - Azure accounts only  
- `--gcp` - GCP accounts only
- *(no flag)* - All providers (default)

### Help
- `--help` - Show help message with examples

## Examples

### Security Team Daily Report
```bash
# Check yesterday's failures across all cloud providers
python report.py --token "your_token_here" --timeframe 1 --f
```

### Weekly AWS Review
```bash
# Get all AWS compliance data from last 7 days
python report.py --token "your_token_here" --timeframe 7 --all --aws
```

### Monthly Azure Audit
```bash
# Focus on Azure failures from last 30 days
python report.py --token "your_token_here" --timeframe 30 --f --azure
```

### Compliance Success Tracking
```bash
# See what's working well in GCP environment
python report.py --token "your_token_here" --timeframe 14 --s --gcp
```

## API Token Setup

### Getting Your API Token

1. **Log into Trend Vision One**
2. **Navigate to Administration > API Keys**
3. **Create a new API key** with Cloud Posture permissions
4. **Copy the token** for use with the script

### Token Security

⚠️ **Important**: Keep your API token secure:
- Never commit tokens to version control
- Use environment variables in automated environments
- Rotate tokens regularly
- Use least-privilege permissions

```bash
# Example with environment variable
export TMV1_TOKEN="your_token_here"
python report.py --token "$TMV1_TOKEN" --f --aws
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
- **Provider filtering**: Reduces unnecessary API calls by filtering accounts first
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
- All accounts filtered out by provider selection
- API token lacks necessary permissions

Solution: Try increasing --timeframe or removing provider filters
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
3. **Try broader filters** (remove provider filtering, increase timeframe)
4. **Contact support** with console output if issues persist

## API Rate Limits

The script respects Trend Vision One API limits:
- **Sequential processing**: Processes accounts one at a time
- **Reasonable delays**: Built-in request pacing
- **Error handling**: Graceful handling of rate limit responses

## Version History

### Current Version
- ✅ Simplified command-line interface
- ✅ Provider-specific filtering (AWS, Azure, GCP)
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
# 2. Run a quick test
python report.py --token "your_token_here" --timeframe 1 --f

# 3. Generate comprehensive weekly report
python report.py --token "your_token_here" --timeframe 7 --all

# 4. Focus on specific cloud provider
python report.py --token "your_token_here" --timeframe 30 --f --aws

# 5. Open the generated Excel file for analysis
```