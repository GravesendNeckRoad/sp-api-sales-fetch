This is an Amazon SP-API sales/inventory report automator. 

Standalone methods allow you to retrieve orders and inventory data from the SP-API. The script has been optimized to avoid throttle, but depending on the report you may need to fine-tune the exponential backoff helper method.

The main compile_report method will run inventory and orders reports and return a simple daily sales/revenue report in .xlsx and emails it to a recipient. Format: columns=['sku', 'title', 'revenue', 'units sold', 'remaining units']. 

Great for a quick glance at what is selling and how you're doing on a day-to-day basis. To use, simply add the required credentials and parameters through config.py, and run main.py.

Oct 24 notes :
-Azure Function CRON code snippet to be added later
-Returns API not working from Amazons end