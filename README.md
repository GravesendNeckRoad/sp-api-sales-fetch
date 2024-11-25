UPD 11/25/2024

- Converted to a durable function to handle orders/inventory/assembly sequentially

- Added query params 'start_date', 'end_date' and 'account_name' to URI. Can now run simple-sales-report for several different report ranges, and for any account.
  The dates can be left blank, and a simple 24H report will run. Though, **the account_name must be present**, else the script wont know where to look for the keys.

- Spruced up docstrings for Utilities, added clarity

EXAMPLE:
https://{functionAppName}.azurewebsites.net/api/orchestrators/{functionName}?code={functionKey}&account_name={name}&start_date={11-25-2024}

See local.settings.example.json and docstring in Utilities/report_tools.py for required environment variables, specific instructions.
