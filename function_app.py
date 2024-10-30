import logging
import os

import azure.functions as func

from report_tools import GenerateFBAReport, BlobHandler, ReportAssembler
from utils import Helpers

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# start at 9:00 AM, retry until success. Logic App triggers at 10:00AM
@app.schedule(schedule="0 35 14 * * *", arg_name="dailysales", run_on_startup=False, use_monitor=True) 
@app.retry(strategy="fixed_delay", max_retry_count=10, delay_interval="00:10:00")
def daily_sales_cron_job(dailysales: func.TimerRequest) -> None:
    if dailysales.past_due:
        logging.info('The timer is past due!')

    # # instantiate class to fetch data - leave params blank for daily sales...
    get_data = GenerateFBAReport() 

    # # get API keys and access token...
    get_data.get_amz_keys()
    get_data.request_access_token()
    
    # # instantiate blob class...
    blob_client = BlobHandler(
        storage_account=os.getenv('STORAGE_ACCOUNT_NAME'), 
        container_name=os.getenv('BLOB_CONTAINER_NAME')
        )

    # # instantiate class to compile reports...
    report = ReportAssembler(account_name='daily')  # using 'daily' for a clean title
    
    # # instantiate utilss class (exponential backoff, memory buffer)...
    utils = Helpers()

    # request inventory and order data...
    # NOTE: requests 'inventory' 1st - its prone to failure - if fails, break & let the app retry
    reports_needed = [
        ('inventory', 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA'),
        ('orders', 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL')
        ]
    
    for report_name, report_type in reports_needed:
        try:
            # request report download from API...
            report_id = get_data.request_FBA_report(report_type)
            
            # check status, download once ready...
            current_attempt = 1
            while current_attempt <= 10:
                status = get_data.check_report_status(report_id)
                
                if status == 'DONE':
                    # get data and write it to a DataFrame...
                    get_data.get_download_url()
                    df = get_data.download_report()
                    break
                
                elif status == 'FATAL':
                    # break the program and let the Function App retry...
                    logging.error(f"'FATAL': could not download {report_name} report: ID={report_id}")
                    raise ValueError(f"'FATAL': could not download {report_name} report: ID={report_id}") 

                else: 
                    # retry if its 'IN_QUEUE' or 'IN_PROGRESS'...
                    # NOTE: keep a longer timer between checks - 'inventory' reports are fragile 
                    utils.exponential_backoff(current_attempt, base_seconds=15, rate_of_growth=1.5)
                    current_attempt += 1

            # write each report to a designated df...
            if report_name == 'inventory':
                i_df = df
            elif report_name == 'orders':
                o_df = df
            logging.info(f"Successfully fetched the '{report_name}' report")

        # let it break and leave the retry logic to the Azure app...
        except Exception as e:
            logging.error(f"Could not generate report for {report_name}: {str(e)}")
            raise

    # assemble pivot table df (Sheet1) and raw orders df (Sheet2)...
    final_dfs = report.simple_sales_report(o_df, i_df)
    summary_df = final_dfs[0]
    raw_data_df = final_dfs[1] 
    
    # visually format with openpyxl (must save df as in-memory obj first)...
    finished_report = report.format_simple_sales_report(df=summary_df, raw_df=raw_data_df)
    
    # upload to blob (Logic App will detect and generate email)...
    report_name = report.set_report_name()
    blob_client.save_to_blob(finished_report, f"{report_name}.xlsx")
    logging.info(f"'{report.report_name}' has been uploaded to {os.getenv('BLOB_CONTAINER_NAME')}")

    logging.info('Python timer trigger function executed.')