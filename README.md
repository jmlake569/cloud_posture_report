# Cloud Posture Report

A Python script for exporting Trend Micro Cloud Posture checks to CSV format with intelligent region-based partitioning for optimal performance.

## Overview

This script connects to the Trend Vision One API to retrieve Cloud Posture Management (CSPM) checks across all your cloud accounts. It automatically partitions data retrieval by region to handle large datasets efficiently and includes adaptive retry logic for reliable API communication.

## Features

- **Multi-Cloud Support**: Works with AWS, Azure, and GCP accounts
- **Region-Based Partitioning**: Automatically discovers and partitions data by cloud regions for optimal performance
- **Adaptive Retry Logic**: Handles API rate limits and temporary failures with exponential backoff
- **Comprehensive Data Export**: Exports all available fields from the Cloud Posture API
- **Resolved Checks Filter**: Only exports checks that have been resolved (have a `resolvedDateTime`)
- **Flexible Time Ranges**: Configurable date ranges for data retrieval
- **Debug Mode**: Optional verbose logging for troubleshooting

## Prerequisites

- Python 3.6 or higher
- Trend Vision One API token with Cloud Posture access
- Network access to `api.xdr.trendmicro.com`

## Installation

1. Clone or download the script:
   ```bash
   git clone <repository-url>
   cd cloud_posture_report
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set your API token as an environment variable:
   ```bash
   export TMV1_TOKEN="your_api_token_here"
   ```

## Usage

### Basic Usage

```bash
python report.py
```

This will:
- Export the last 30 days of resolved Cloud Posture checks
- Use a page size of 200 records per API call
- Save results to `cloud_posture_checks.csv`

### Advanced Usage

```bash
python report.py --days 90 --top 100 --outfile my_report.csv --debug
```

### Command Line Options

| Option | Description | Default | Range |
|--------|-------------|---------|-------|
| `--days` | Number of days back from now to retrieve | 30 | > 0 |
| `--top` | Page size for API requests | 200 | 50-200 |
| `--outfile` | Output CSV filename | `cloud_posture_checks.csv` | Any valid filename |
| `--debug` | Enable verbose logging | False | N/A |

## How It Works

### 1. Account Discovery
The script first retrieves all cloud accounts from your Trend Vision One environment.

### 2. Region-Based Partitioning
For each account, the script:
- Samples regions from the first API response
- Probes common regions for the specific cloud provider (AWS/Azure/GCP)
- Fetches data separately for each discovered region

### 3. Adaptive Performance
- Automatically reduces page size on API errors (429, 502, 503, 504)
- Implements exponential backoff for retries
- Handles JSON decode errors gracefully

### 4. Data Processing
- Filters out unresolved checks (no `resolvedDateTime`)
- Flattens complex data structures (lists/dicts) to JSON strings
- Exports all available fields dynamically

## Output Format

The CSV file contains all fields returned by the Cloud Posture API, including:

- `accountId` - The cloud account identifier
- `accountName` - The cloud account name
- `ruleId` - The compliance rule identifier
- `status` - Check status (SUCCESS/FAILURE)
- `riskLevel` - Risk level (LOW/MEDIUM/HIGH/VERY_HIGH/EXTREME)
- `region` - Cloud region
- `service` - Cloud service (EC2, S3, etc.)
- `resource` - Affected resource identifier
- `description` - Check description
- `resolvedDateTime` - When the check was resolved
- `categories` - Compliance categories
- `compliances` - Applicable compliance frameworks

## Performance Considerations

- **Large Datasets**: The script is optimized for accounts with 10K+ checks through region partitioning
- **API Limits**: Respects Trend Vision One API rate limits with adaptive backoff
- **Memory Usage**: Processes data in chunks to minimize memory consumption
- **Network Resilience**: Handles temporary network issues and API failures

## Troubleshooting

### Common Issues

1. **Missing API Token**
   ```
   ❌ ERROR: Missing required environment variable 'TMV1_TOKEN'.
   ```
   **Solution**: Set the `TMV1_TOKEN` environment variable with your API token.

2. **API Authentication Errors**
   ```
   ❌ HTTP Error fetching accounts: 401 - Unauthorized
   ```
   **Solution**: Verify your API token is valid and has Cloud Posture permissions.

3. **Network Timeouts**
   ```
   ⚠️ Timeout, retry 1/3 after 2.3s with top=100
   ```
   **Solution**: The script will automatically retry with smaller page sizes. If persistent, check your network connection.

### Debug Mode

Enable debug mode to see detailed API responses and internal processing:

```bash
python report.py --debug
```

This will show:
- Raw API payloads (truncated)
- Region discovery process
- Retry attempts and backoff timing

## Security Notes

- Store your API token securely and never commit it to version control
- Use environment variables rather than hardcoding tokens
- The script only reads data and does not modify any cloud resources
- API tokens should have minimal required permissions (read-only access to Cloud Posture)

## API Reference

This script uses the Trend Vision One Cloud Posture API:
- **Base URL**: `https://api.xdr.trendmicro.com`
- **Endpoints**: 
  - `/beta/cloudPosture/accounts` - List cloud accounts
  - `/beta/cloudPosture/checks` - Retrieve compliance checks
- **Authentication**: Bearer token via `Authorization` header

## License

[Add your license information here]

## Support

For issues related to:
- **Script functionality**: Check the troubleshooting section above
- **API access**: Contact your Trend Micro administrator
- **Cloud Posture data**: Refer to Trend Vision One documentation
