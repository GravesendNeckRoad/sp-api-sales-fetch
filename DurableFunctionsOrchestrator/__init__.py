from datetime import timedelta

from azure.durable_functions import DurableOrchestrationContext, Orchestrator, RetryOptions


def orchestrator_function(context: DurableOrchestrationContext):
    retry_policy = RetryOptions(first_retry_interval_in_milliseconds=(300), max_number_of_attempts=3)
    
    # get the query params dict from the HTTP trigger
    query_params = context.get_input()
        
    try:
        # get orders data from API
        orders = yield context.call_activity_with_retry('Activity_Orders', retry_policy, query_params)
        yield context.create_timer(context.current_utc_datetime + timedelta(seconds=3))

        # get inventory data from API
        inv = yield context.call_activity_with_retry('Activity_Inventory', retry_policy, query_params)
        yield context.create_timer(context.current_utc_datetime + timedelta(seconds=3))
        
        # pack results into a dict
        results = {
            'account_name': query_params['account_name'],
            'orders': [orders],
            'inventory': [inv]        
        }
        
        # pass results to assembler to assemble report and upload to storage acc 
        yield context.call_activity('Activity_Assembler', results)

        # safely exit    
        context.set_custom_status("Completed")
        return None
    
    # basic error handler (break and fall back on retry_policy if anything)
    except Exception as e:
        context.set_custom_status(f"Failure: {str(e)}")
        raise

main = Orchestrator.create(orchestrator_function)