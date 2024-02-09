# Trend Micro Cloud One - Conformity Audit Script

This Python script automates the process of auditing cloud infrastructure compliance using Trend Micro's Cloud One - Conformity API. It is designed to fetch compliance check data for accounts and generate a detailed report categorizing each check as either a success or a failure. Additional details such as "failure-introduced-by", "resolved-by", and "risk-level" are also included to provide comprehensive insights into the compliance status.

## Prerequisites

Before using this script, you should have:

- Python 3.x installed on your system.
- Access to Trend Micro Cloud One - Conformity and a read-only API key.

## Installation

1. Ensure you have Python 3.x installed. You can download it from [python.org](https://www.python.org/downloads/).

2. Install the required Python libraries:

   ```bash
   pip install requests pandas

# Trend Micro Cloud One - Conformity Compliance Data Retrieval Script

This script retrieves compliance data from Trend Micro Cloud One - Conformity using a read-only API key.

## Configuration

Open the script in a text editor.

Locate the following lines at the top of the script:

```python
C1_API_KEY = "your_api_key_here"
DAYS_BACK = 30  # Adjust this value to change the date range for fetching data.
```

Replace `"your_api_key_here"` with your actual Trend Micro Cloud One - Conformity API key.

Adjust `DAYS_BACK` if you wish to change the time frame for the compliance data retrieval.

## Usage

To run the script, navigate to the directory containing the script and execute:

```bash
python report.py
```

Replace `report.py` with the actual name of your Python script file.

## Output

The script generates an Excel file named `compliance_report.xlsx` with two sheets:

- **Failures**: Lists all compliance checks that have failed, along with details such as "failure-introduced-by" and "risk-level".
- **Successes**: Lists all compliance checks that have passed, including the "resolved-by" detail.

## Logging

The script logs its operations, including successful executions and any errors encountered, to a file named `api_audit.log`.

## References

- [Trend Micro Cloud One](https://www.trendmicro.com/en_us/business/products/hybrid-cloud/cloud-one-conformity.html)
- [API Reference](https://cloudone.trendmicro.com/docs/conformity/api-reference/)

## Disclaimer

This script is provided "as is", without warranty of any kind. Use it at your own risk.

## License

Specify your license or indicate if the script is available under an open-source license.

Make sure to replace placeholders like `report.py` with the actual name of your script and adjust any instructions or descriptions as necessary to fit your specific implementation or environment requirements.