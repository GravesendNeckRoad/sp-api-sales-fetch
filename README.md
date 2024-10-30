UPD 10/30/24

New branch specifically for use with Azure Functions. Added integration for Key Vault, Blob containers. Removed email class/funcitonality as Functions does not have SMTP functionality (substituting with Logic App).

Main branch still a great option to run quick ETL automations on local machines, and automate emails.
____

Daily Sales Automation strategy: 

CRON trigger activates once daily. The '...UNSUPRESSED_INVENTORY' SP-API report is prone to failure when requesting, as it generates once only every 30 minutes, and our team does heavy testing with it. 
One option was to simply pull the last available report, but as this is a live sales report, we need the most recent inventory data. Therefore, this script runs the inventory report first - if it fails, it retries until success. 
Upon success, finished report is saved to Blob container. Logic App then detects trigger, and generates email containing the report as attachment. 

BUGS:
CRON triggers don't allow for retries, must reupload function as an HTTP trigger and set up a Logic App to trigger it each morning, in order to obtain access to the Function App built-in retries 
(due to the nature of this reporting - waiting for the reports to generate on Amazon's end - runtime can exceed 5 minutes if I bake retries into the code itself, hence the Function App retries are integral)

