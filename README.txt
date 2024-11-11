This is a simple ETL pipeline for Amazon SP-API sales/inventory reports.

Standalone methods allow you to retrieve orders/inventory data from the SP-API. The main script has been optimized to avoid throttle, but depending on the report you may need to fine-tune the exponential backoff helper method.

The main compile_report method runs inventory and orders reports and returns a simple daily sales/revenue report as .xlsx and emails it to a recipient. Report format: columns=['sku', 'title', 'revenue', 'units sold', 'remaining units']. 

Great for a quick glance at what is selling and how you're doing on a day-to-day basis. To use, simply add the required credentials and parameters through config.py, and run main.py.

*To be used on local machines only - SMTP not be able to send email through cloud apps.

Oct '24 notes :
-Azure Function CRON code snippet to be added later
-Returns API not working from Amazons end
