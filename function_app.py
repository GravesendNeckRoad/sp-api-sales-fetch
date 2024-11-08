import logging
import os

import azure.functions as func

from report_tools import GenerateFBAReport, BlobHandler, ReportAssembler
from utils import Helpers

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="daily_sales_trigger")
def daily_sales_trigger(req: func.HttpRequest) -> func.HttpResponse:
    # instantiate class to fetch data - leave params blank for daily sales...
    get_data = GenerateFBAReport() 

    # get API keys and access token...
    get_data.get_amz_keys()
    get_data.request_access_token()
    
    # instantiate blob class...
    blob_client = BlobHandler(
        storage_account=os.getenv('STORAGE_ACCOUNT_NAME'), 
        container_name=os.getenv('BLOB_CONTAINER_NAME')
        )

    # instantiate class to compile reports...
    report = ReportAssembler(account_name='daily')  # using 'daily' for a clean title
    
    # instantiate utils class (exponential backoff, memory buffer)...
    utils = Helpers()
            
    # request inventory and order data...
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
            while current_attempt <= 7:
                status = get_data.check_report_status(report_id)
                
                if status == 'DONE':
                    # get data and write it to a DataFrame...
                    get_data.get_download_url()
                    df = get_data.download_report()
                    break
                
                # if fatal, use a recently generated backup from the getreports API instead 
                elif status == 'FATAL':
                    logging.error(f"'FATAL': could not download {report_name} for report ID {report_id}")
                    logging.info(f"Attempting to fetch {get_data.report_type} backup from recently generated reports")
                    new_report_id = get_data.get_last_ready_report_id()
                    logging.info(f"Obtained backup for '{get_data.report_type}'. New report ID: {new_report_id}")
                    get_data.get_download_url()
                    df = get_data.download_report()
                    break

                else: 
                    # retry if its 'IN_QUEUE' or 'IN_PROGRESS'...
                    utils.exponential_backoff(current_attempt, base_seconds=15, rate_of_growth=1.5)
                    current_attempt += 1

            # write each report to a designated df...
            if report_name == 'inventory':
                i_df = df
            elif report_name == 'orders':
                o_df = df
            logging.info(f"Successfully fetched the '{report_name}' report")

        # if anything goes wrong, let it break and leave the retry logic to the Azure app...
        except Exception as e:
            logging.error(f"Could not generate report for {report_name}: {str(e)}")
            raise

    # assemble pivot table df (Sheet1) and raw orders df (Sheet2)...
    final_dfs = report.simple_sales_report(o_df, i_df)
    summary_df = utils.save_df_to_mem(final_dfs[0])  # can pass a buffer object,
    raw_data_df = final_dfs[1]  # or just a df
    
    # visually format with openpyxl...
    finished_report = report.format_simple_sales_report(df=summary_df, raw_df=raw_data_df)
    
    # upload to blob (Logic App will detect and generate email)...
    report_name = report.set_report_name()
    blob_client.save_to_blob(finished_report, f"{report_name}.xlsx")
          
    return func.HttpResponse(
        f"Report '{report.report_name}' has been uploaded to container {os.getenv('BLOB_CONTAINER_NAME')}",
        status_code=200
        )

