import io 
import os
from typing import List, TypedDict

import pandas as pd

from Utilities.report_tools import ReportAssembler
from Utilities.utils import BlobHandler

class CompilerDict(TypedDict):
    account_name: str
    orders: List[str]
    inventory: List[str]

def main(name: CompilerDict) -> None:
    """Compiles Activity_Orders and Activity_Inventory into a finished report and uploads to blob container"""
            
    # pull results payload from orchestrator, after running Activity_Orders and Activity_Inventory
    account_name = name.get('account_name')
    orders = name.get('orders')
    inventory = name.get('inventory')
    
    # instantiate classes     
    compiler = ReportAssembler(account_name=account_name)
    blobber = BlobHandler(
        storage_account=os.getenv('STORAGE_ACCOUNT_NAME'),
        container_name=os.getenv('DAILY_SALES_BLOB_CONTAINER_NAME')
        )

    # convert json lists to unified dfs 
    orders = [pd.read_json(io.StringIO(order)) for order in orders]    
    orders = pd.concat(orders, ignore_index=True).drop_duplicates()
    
    inventory = [pd.read_json(io.StringIO(inv)) for inv in inventory]           
    inventory = pd.concat(inventory, ignore_index=True).drop_duplicates()
    
    # process daily sales report (returns tuple with finished report, and raw orders data)
    finished_df, raw_orders = compiler.simple_sales_report(orders_df=orders, inventory_df=inventory)
    
    # pass daily sales tuple to visual formatter (returns an io obj)
    final_report_buffer = compiler.format_simple_sales_report(df=finished_df, raw_df=raw_orders)
    
    # title report (method returns titled account name, switching to upper() since using initials)
    report_name = (compiler.set_simple_sales_report_name()).replace(account_name.title(), account_name.upper())
    
    # save to blob container
    blobber.save_to_blob(buffer=final_report_buffer, save_as=f"{report_name}.xlsx")
    
    return None
