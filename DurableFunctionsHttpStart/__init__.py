import logging

from azure.functions import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient


async def main(req: HttpRequest, starter: str) -> HttpResponse:
        
    # ensure an account name is passed to the request uri 
    account_name = req.params.get('account_name')   
    if not account_name:
        return HttpResponse("Missing 'account_name' parameter in the URI", status_code=400)
    
    # optional date query params (if omitted will default to 24H report by passing None down to activities)
    start_date = req.params.get('start_date', None)
    end_date = req.params.get('end_date', None)
    
    # assemble payload out of the inputs to pass down to orchestrator
    query_params = {
        'account_name': account_name,
        'start_date': start_date,
        'end_date': end_date
    }
        
    # pass payload to the main orchestrator 
    client = DurableOrchestrationClient(starter)
    
    instance_id = await client.start_new('DurableFunctionsOrchestrator', None, query_params)
    
    logging.info(f"Started orchestration with ID = '{instance_id}'.")
    
    return client.create_check_status_response(req, instance_id)