import logging
from typing import TypedDict

from Utilities.report_tools import ReportDownloadOrchestrator
from Utilities.utils import Helpers

class InputPayload(TypedDict):
    account_name: str
    start_date: str
    end_date: str

def main(name: InputPayload) -> str:
    """Fetches the inventory data from Amazon API and returns the resulting data-table as a json str"""    
    
    # unpack the query params input
    account_name = (name.get('account_name')).upper()
    start_date = name.get('start_date')
    end_date = name.get('end_date')

    logging.info(f"Now processing Activity_Inventory for {account_name}")
            
    # instantiate classes 
    fetch = ReportDownloadOrchestrator(account_name=account_name)
    backoff = Helpers()
    
    # generate orders report
    current_attempt = 1
    max_attempts = 5
    while current_attempt <= max_attempts:
        try:
            inv_json = fetch.get_report(
                start_date=start_date,
                end_date=end_date,
                report_type='GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA'
            )
            return inv_json
        
        # basic error catching - everything else is handled in utils
        except Exception as e:
            logging.error(f"Failure to get report in Activity_Inventory on attempt {current_attempt}: {str(e)}")
            backoff.exponential_backoff(current_attempt)
            current_attempt += 1

    # break if all else fails, orchestrator will restart             
    if current_attempt > max_attempts:
        logging.error(f"Activity_Inventory: Could not fetch report after {max_attempts} tries")
        raise Exception(f"Activity_Inventory: Could not fetch report after {max_attempts} tries")