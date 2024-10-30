import gzip
import io
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union

import openpyxl as xl
import pandas as pd
import pytz
import requests as req

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

from utils import Helpers, Style

class ZeroSalesError(Exception):
    "Raises appropriate exception in case of an empty df"
    pass

class GenerateFBAReport:       
    """
    Requests and downloads reports from the Amazon SP-API

    Parameters:
        -start_date (str, optional): The starting date of your report (format: 'mm-dd-yyyy')
        -end_date (str, optional): The end date of your report (format: 'mm-dd-yyyy')
        
    Requirements: 
        -AZ Key Vault with SP-API keys: ['client secret', 'client id', 'refresh token', 'rotation deadline']    
        
        -The following Environment Variables created in your Function App settings:
            VAULT_NAME: The name of the Key Vault
            CLIENT_ID: The key name of the 'client id' secret
            CLIENT_SECRET: The key name of the 'client secret' secret
            REFRESH_TOKEN: The key name of the 'refresh token' secret
            ROTATION_DEADLINE: the key name of the 'rotation deadline' secret
            MARKETPLACE_ID: For North America: 'ATVPDKIKX0DER'
            ENDPOINT: Reports endpoint: 'https://sellingpartnerapi-na.amazon.com/reports/2021-06-30'
            TOKEN_REQUEST_URL: Access token endpoint: 'https://api.amazon.com/auth/o2/token'

    Considerations: 
        -Defaults to daily report (last 24 hours) if both parameters are left blank
        
        -Maximum report date range is 31 days
        
        -This class uses `DefaultAzureCredential` authentication, so ensure your managed identities are in order

        -Eager-validates all keys and environment variables, so ensure that the above specified
        requirements are all satisfied

        -Full list of available reports to generate using this class: 
        https://developer-docs.amazon.com/sp-api/docs/report-type-values-fba    
    """
    def __init__(self, start_date: str = None, end_date: str = None):
        
        # validating date range input
        self.start_date_iso, self.end_date_iso = self.__validate_user_input(start_date, end_date)
        
        # validating environment variables 
        self.__validate_environment_variables()
        
        # validating key vault
        self.key_vault = self.__init_key_vault()  # must init a kv before validating it 
        self.__validate_key_vault()

        # api keys
        self.client_secret = None
        self.refresh_token = None
        self.client_id = None  
        self.rotation_deadline = None

        # utils
        self.backoff = Helpers()
        self.reports_url = os.getenv("ENDPOINT")
        self.access_token = None
        self.report_id = None 
        self.report_endpoint = None
        self.download_url = None
        self.compression = None

    @staticmethod
    def __validate_user_input(start_date: str, end_date: str) -> Tuple[str, str]:
        """Private method: validates ['start_date', 'end_date'] constructor parameters upon instantiation"""
        try:            
            today = datetime.now(pytz.timezone('US/Eastern')).astimezone(pytz.utc)
            yesterday = (today - timedelta(days=1))

            # if start date is left blank, default to yesterday, otherwise validate user input
            if start_date is None:
                start_date_formatted = yesterday
            else:
                start_date_cleaned = re.sub(r"[/.]", "-", start_date)
                start_date_formatted = datetime.strptime(start_date_cleaned, '%m-%d-%Y')
                start_date_formatted = pytz.timezone('US/Eastern').localize(start_date_formatted)

            # if end date is left blank, default to today, otherwise validate user input
            if end_date is None:
                end_date_formatted = today
            else:
                end_date_cleaned = re.sub(r"[/.]", "-", end_date)
                end_date_formatted = datetime.strptime(end_date_cleaned, '%m-%d-%Y')
                end_date_formatted = pytz.timezone('US/Eastern').localize(end_date_formatted)

            # misc basic validation 
            if start_date_formatted >= end_date_formatted:
                raise ValueError("The start_date must occur before the end_date")

            if end_date_formatted > today:
                raise ValueError("The end_date cannot be later than today's date")

            report_length_in_days = (end_date_formatted - start_date_formatted).days
            if report_length_in_days > 31:
                raise ValueError("The reports API can only generate a date range of up to 31 days long")

            # Amazon takes ISO format datetimes, format as such
            start_date_iso = start_date_formatted.isoformat()
            end_date_iso = end_date_formatted.isoformat()
            logging.info("Successfully validated the date parameters")
            return start_date_iso, end_date_iso

        except Exception as e:
            logging.error(f"Could not validate your date parameters: {str(e)}")
            raise
        
    def __validate_environment_variables(self) -> None:
        """Private method: validates the Function App environmental variables required to run this script"""

        env_vars_needed_to_run_program = [
            "VAULT_NAME",
            "CLIENT_ID",
            "CLIENT_SECRET",
            "REFRESH_TOKEN",
            "TOKEN_REQUEST_URL",
            "MARKETPLACE_ID",
            "ENDPOINT",
            "ROTATION_DEADLINE"
        ]

        # break program if any env vars are missing, log to user
        missing_vars = list()        
        for var_name in env_vars_needed_to_run_program:
            env_var = os.getenv(var_name)            
            if not env_var or len(env_var) == 0:
                missing_vars.append(var_name)
    
        if len(missing_vars) > 0:
            logging.error(f"Missing the following environmental variables: {missing_vars}")
            raise ValueError(f"Missing the following environmental variables: {missing_vars}")
        else:
            logging.info("Successfully validated all necessary environment variables")

    def __validate_key_vault(self) -> None:
        """Private method: validates the existence of the required keys in the Key Vault"""
        
        if not self.key_vault:
            raise ValueError("Must first initialize an instance of the key vault")

        keys_needed_to_run_app = [
            "CLIENT_SECRET",
            "CLIENT_ID",
            "REFRESH_TOKEN",
            "ROTATION_DEADLINE"
        ]
        
        # making sure the key names exist and match to the env var names you set them as
        missing_keys = []
        for env_var in keys_needed_to_run_app:
            try:
                key_name = os.getenv(env_var)
                self.key_vault.get_secret(key_name)
                
            except Exception as e:
                missing_keys.append(env_var)
                logging.error(f"Could not fetch secret for {env_var}: {str(e)}")

        if missing_keys:
            raise ValueError(f"Missing the following keys from Key Vault: {missing_keys}")
        else:
            logging.info("Successfully fetched and validated all required keys")

    def __init_key_vault(self) -> SecretClient:
        """Private method: Returns Secret Client object that then lets you access secrets"""
        
        vault_name = os.getenv('VAULT_NAME')
        if not vault_name or len(vault_name) == 0:
            raise ValueError('Could not locate VAULT_NAME environmental variable')
        
        try:             
            secret_client = SecretClient(
                vault_url=f"https://{vault_name}.vault.azure.net",
                credential=DefaultAzureCredential()
            )
            
            logging.info("Successfully initialized and accessed the Key Vault")                
            return secret_client 
       
        except Exception as e:
            logging.error(f"Could not initialize the key vault client.{str(e)}")
            raise

    def fetch_from_key_vault(self, key_name: str) -> str:
        """
        Retrieves the secret value for a key name, from the vault specified by the "VAULT_NAME" environment variable
                
        :param (str) key_name: The key name whos secret you wish to retrieve from the Vault 
        :return (str) The secret value string of the key name passed
        """

        if not self.key_vault:
            raise ValueError("Must first initialize an instance of the key vault")
        
        if not key_name or len(key_name) == 0:
            raise ValueError("Please provide a key_name parameter")

        try:        
            secret = self.key_vault.get_secret(key_name)
            return secret.value

        except Exception as e:
            logging.error(f"Could not fetch the secret value for {key_name}.{str(e)}")
            raise

    def get_amz_keys(self) -> None:
        """Populates client_id, client_secret, rotation_deadline and refresh_token attributes for accessing SP-API"""

        if not self.key_vault:
            raise ValueError("Must first initialize an instance of the key vault")
                
        keys_dict = {
            'client_id': os.getenv("CLIENT_ID"),
            'client_secret': os.getenv("CLIENT_SECRET"),
            'refresh_token': os.getenv("REFRESH_TOKEN"),
            'rotation_deadline': os.getenv("ROTATION_DEADLINE")
        }
        
        # populate instance attributes with the key secrets
        for k, v in keys_dict.items():
            try:
                secret_value = self.fetch_from_key_vault(v) 
                setattr(self, k, secret_value)
                logging.info(f"Successfully fetched key for {k}")   

            # break program if any key names are missing                                  
            except Exception as e:
                logging.error(f"Could not fetch key for {k}: {str(e)}")
                raise

        logging.info("Successfully fetched all keys from vault")
        
        # terminate if the SP-API keys have expired
        key_expiration_date = datetime.strptime(self.rotation_deadline, "%Y-%m-%d").date()
        if datetime.today().date() >= key_expiration_date:
            raise ValueError(
                "Your API keys have expired. Please generate new ones via the SellerCentral portal"
                )

    def request_access_token(self) -> str:
        """Requests LWA access token from the SP-API, and returns the token as a string"""

        if not any([self.client_id, self.client_secret, self.refresh_token]):
            logging.error("Must populate the key vault instance attributes before requesting an access token")
            raise ValueError("Must populate the key vault instance attributes before requesting an access token")

        token_request_url = os.getenv('TOKEN_REQUEST_URL')   

        if not token_request_url or len(token_request_url) == 0:
            logging.error("No environment variable for TOKEN_REQUEST_URL was located")
            raise ValueError("No environment variable for TOKEN_REQUEST_URL was located")
        
        max_retries = 5
        current_attempt = 1
        
        while current_attempt <= max_retries:
            try:
                token_request = req.post(
                    url=token_request_url,
                    timeout=15,
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": self.refresh_token,
                        "client_id": self.client_id,
                        "client_secret": self.client_secret                
                    }
                )
                
                if token_request.status_code == 200:    
                    self.access_token = token_request.json().get('access_token', '')
                    logging.info("Successfully fetched request token")
                    return self.access_token
                
                elif token_request.status_code in [400, 401, 403, 404]:
                    logging.error(f"{token_request.status_code} Error, couldn't fetch LWA token")
                    raise RuntimeError(f"{token_request.status_code} Error, couldn't fetch LWA token")
                    
                else:
                    logging.error(f"{token_request.status_code} Error, failed to retrieve LWA token")
                    self.backoff.exponential_backoff(current_attempt)
                    current_attempt += 1

            except Exception as e:
                logging.error(f"Failed to request LWA token: {str(e)}")
                self.backoff.exponential_backoff(current_attempt)
                current_attempt += 1
        
        logging.error(f"Couldn't fetch access token after {max_retries} attempts")
        raise RuntimeError(f"Could not fetch the access token after {max_retries} attempts")
    
    def request_FBA_report(self, report_type: Optional[str] = None) -> str:
        """
        Requests a downloadable report from the SP-API reports endpoint
        
        Parameters:
            report_type (str, optional): Default = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'

        Returns:
            report_id (str) The report_id of the report you requested
            Also populates report_id instance attribute
        """        
        if self.access_token is None:
            raise ValueError('No access token detected. Run the request_access_token() method first')
        
        # default to orders report if param is left blank
        if not report_type:
            report_type = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'

        # inv report doesn't take date params, but they dont break it either
        report_params = {
            'marketplaceIds': [os.getenv("MARKETPLACE_ID")],
            'reportType': report_type,
            'dataStartTime': self.start_date_iso,
            'dataEndTime': self.end_date_iso
            }

        current_attempt = 1 
        max_attempts = 5        
        while current_attempt <= max_attempts:
            try:                
                report_endpoint = self.reports_url + '/reports' 
                request_download = req.post(
                    url=report_endpoint,
                    headers={'x-amz-access-token': self.access_token},
                    timeout=15,
                    json=report_params
                )
                                
                if request_download.status_code == 202:
                    self.report_id = request_download.json().get('reportId')           
                    logging.info(f"Successfully requested download for {report_type}: report ID {self.report_id}")
                    return self.report_id

                elif request_download.status_code in [400, 401, 403, 404]:
                    logging.error(f"{request_download.status_code} Error for report ID {self.report_id}")
                    raise RuntimeError(
                        f"{request_download.status_code} Error for report ID {self.report_id}"
                        )

                else:
                    logging.error(f"{request_download.status_code} Error for report ID {self.report_id}")
                    Helpers.exponential_backoff(current_attempt)
                    current_attempt += 1
            
            except Exception as e:
                logging.exception(f"Error attempting to request report: {str(e)}")
                Helpers.exponential_backoff(current_attempt)
                current_attempt += 1

        if self.report_id is None:
            logging(f"Maximum allotted retries reached, could not request report '{report_type}'")
            raise ValueError(f"Maximum allotted retries reached: could not request report '{report_type}'")
        else:
            self.report_endpoint = self.reports_url + f"/reports/{self.report_id}"

    def check_report_status(self, report_id: Optional[str] = None) -> str:
        """
        Returns status of the report_id passed from instance attribute or parameter 
        
        Parameters:
            report_id (Optional[str]): Report ID for which to check the status
            If left blank, will default to the current report_id instance attribute
            Otherwise, will check status for that report id, but wont alter current instance attribute
        """
        if self.access_token is None:
            raise ValueError("No access token located. Need to run the `request_access_token` method first")

        if self.report_id is None and report_id is None:
            raise ValueError("No report_id located Run `request_FBA_inventory` or provide a report id parameter")
        
        # use parameter if provided, otherwise use instance attribute (doesn't overwrite instance attributes btw) 
        current_report_id = report_id if report_id else self.report_id
        current_endpoint = self.reports_url + f"/reports/{current_report_id}"
                
        try:
            request_status = req.get(
                url=current_endpoint,
                timeout=15,
                headers={'x-amz-access-token': self.access_token}
                )

            if request_status.status_code != 200:
                # dont break, since retry logic is handled outside of the method
                logging.error(f"{request_status.status_code} Error: failed to get request status")
                status = 'N/A'  

            else:
                status = request_status.json().get('processingStatus')
                logging.info(f"Report {self.report_id} status: {status}")
                
            return status
        
        except Exception as e:
            logging.exception(f'Unexpected error occurred trying to get report status {str(e)}')
            status = 'N/A'
            return status
    
    def get_download_url(self, report_id: Optional[str] = None) -> Tuple[str, str]:
        """
        Fetches the download_url for the report_id, assuming report status is 'DONE'
        
        Parameters:
            report_id: Optional[str] - Report id you wish to get download URL for
            If left blank, will default to the current report_id instance attribute and
            populates the compression and download_url attributes
            Otherwise, returns download_url and compression without updating the instance attributes
    
        Returns: Tuple:
            download_url (str) The link to the report which you can then download
            compression (str) The compression of the downloadable file    
        """
        if self.report_id is None and report_id is None:
            raise ValueError("No report_id attribute located. Run `request_FBA_report` or manually input a report ID")

        if self.access_token is None:
            raise ValueError("No access token located. Need to run the `request_access_token` method first")

        # use parameter if provided, otherwise use instance attribute (doesn't overwrite instance attributes btw) 
        current_report_id = report_id if report_id else self.report_id
        current_endpoint = self.reports_url + f"/reports/{current_report_id}"
        
        # block 1: obtain document ID 
        try:
            request_document_id = req.get(
                url=current_endpoint,
                timeout=15,
                headers={'x-amz-access-token': self.access_token}
            )
        
            if request_document_id.status_code != 200:
                raise RuntimeError(f"{request_document_id.status_code} Error: failed to retrieve document ID")
            
            document_id = request_document_id.json().get('reportDocumentId', '')

            # just making sure you didn't get back a blank document ID        
            if len(document_id) == 0:
                raise ValueError(f'Document ID for report {current_report_id} generated, but was returned empty')

        except Exception as e:
            logging.error(f"Failure fetching document ID from report ID {current_report_id}: {str(e)}")
            raise 

        # block 2: obtain download URL 
        try:
            download_request = req.get(
                url=self.reports_url + f"/documents/{document_id}",
                headers={'x-amz-access-token': self.access_token},
                timeout=15
                )
            
            if download_request.status_code != 200:
                raise RuntimeError(f"Failed to request download: {download_request.status_code}")
            
            download_url = download_request.json().get('url')
            compression = download_request.json().get('compressionAlgorithm', 'No compression')
            
            # only update attributes if no parameter was passed 
            if not report_id:
                self.download_url = download_url
                self.compression = compression

            return download_url, compression
            
        except Exception as e:
            logging.error(f"Retrieved document ID {document_id}, but could not obtain a download URL: {str(e)}")
            raise
    
    def download_report(self, download_url: Optional[str] = None, compression: Optional[str] = None) -> pd.DataFrame:
        """
        Downloads the contents from a given download_url, returns as Pandas DataFrame
        
        Parameters:
            download_url Optional[str] - If specified, will download from provided URL
            Else, will default to the download_url instance attribute
            In the case of the former, instance attribute will not be changed
        
        Returns:
            pd.DataFrame with the downloaded data
        """
        if self.access_token is None:
            raise ValueError("No access token located. Need to run the `request_access_token` method first")
           
        if self.download_url is None and download_url is None:
            raise ValueError("No download URL located. Run `get_download_url` or provide a download_url parameter")

        if self.compression is None and compression is None:
            raise ValueError("Must provide a compression type")
        
        # if parameter is passed, use it, otherwise default to instance attributes
        current_download_url = download_url if download_url else self.download_url
        current_compression = compression if compression else self.compression
    
        # block 1: request the download contents 
        attempt = 1
        max_attempts = 5
        download_successful = False
        while attempt <= max_attempts:
            try:
                download = req.get(
                    url=current_download_url, 
                    stream=True, 
                    timeout=15
                    )
                
                if download.status_code in [400, 401, 403, 404]:
                    raise RuntimeError(f"{download.status_code} Error, could not download report")

                elif download.status_code != 200:
                    logging.error(f"{download.status_code} Error, could not download report")
                    self.backoff.exponential_backoff(attempt)
                    attempt += 1                                
                else:
                    logging.info(f"Download prepared, now decompressing and writing to df")
                    download_successful = True
                    break
                    
            except Exception as e:
                logging.error(f"Failed requesting download for the report on attempt {attempt}: {str(e)}")
                self.backoff.exponential_backoff(attempt)
                attempt += 1

        if not download_successful:
            raise RuntimeError(f"Couldn't download from URL {current_download_url} after {max_attempts} attempts")
        
        # block 2: write contents to df
        try:
            if current_compression == 'No compression':
                report_contents = download.text
            elif current_compression == 'GZIP':
                buffer = io.BytesIO(download.content)
                buffer.seek(0)
                with gzip.GzipFile(fileobj=buffer) as gz:
                    report_contents = gz.read().decode('latin1')
                
            df = pd.read_csv(io.StringIO(report_contents), sep='\t', encoding='latin1')
            return df       

        except Exception as e:
            logging.error(f"Downloaded report from {current_download_url} but could not process to df: {str(e)}")
            raise    

class BlobHandler:
    """
    Instantiates BlobServiceClient and writes data to/from a specified blob container
    
    Parameters:
        -storage_account: (str) Name of the storage account you wish to pull from
        -container_name: (str) Name of the blob container within the above specified storage account 
    
    Considerations:
        -This class uses DefaultAzureCredential(), so make sure your managed identities are in order
    """
    def __init__(self, storage_account: str, container_name: str):
        self.storage_account = storage_account
        self.container_name = container_name
        self.blob_service_client = self.__init_blob_client()
    
    def __init_blob_client(self) -> BlobServiceClient:
        """Private method: initiates and validates a blob client upon class instantiation. Returns client object"""        
        try:
            return BlobServiceClient(
                account_url=f"https://{self.storage_account}.blob.core.windows.net/", 
                credential=DefaultAzureCredential()
                )
        except Exception as e:
            logging.error(f"Could not validate the BlobServiceClient: {str(e)}")
            raise
            
    def save_to_blob(self, buffer: io.BytesIO, save_as: str) -> None:
        """Uploads in-memory buffer file to the blob container, titled after the save_as parameter"""
        
        if not isinstance(buffer, io.BytesIO):
            raise TypeError("The data passed to this method must be of io.BytesIO type")

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=save_as
                )
            blob_client.upload_blob(buffer, overwrite=True)
            logging.info(f"Uploaded file {save_as} to blob")

        except Exception as e:
            logging.error(f"Could not save {save_as} to blob. {str(e)}")
            raise
   
    def get_from_blob(self, blob_name: str) -> pd.DataFrame:
        """Transfer from blob to local machine, returns Pandas df. Blob name should include file extension"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
                )
            blob_data = blob_client.download_blob().readall()

            if blob_name.endswith('xlsx'):
                df = pd.read_excel(io.BytesIO(blob_data), engine='openpyxl')
            elif blob_name.endswith('csv'):
                df = pd.read_csv(io.BytesIO(blob_data))
            elif blob_name.endswith('tsv'):
                df = pd.read_csv(io.BytesIO(blob_data), sep='\t')
            elif blob_name.endswith('txt'):
                txt_file = io.BytesIO(blob_data).read().decode('utf-8')
                df = pd.DataFrame(txt_file.splitlines())
            else:
                raise TypeError("Only supports xlsx/csv/tsv/txt files for now")

            return df
        
        except Exception as e:
            logging.error(f"Error getting your file from blob. {str(e)}")
            raise
            

class ReportAssembler:
    """
    Compiles and styles/formats DataFrames and .xlsx files/reports
    
    Parameters:
        -account_name: Optional[str] - adds an account name to the report title
    """
    
    def __init__(self, account_name: Optional[str] = None):
        
        # these will be used to define the report name 
        self.account_name = account_name        
        self.date_start = None
        self.date_end = None
        self.date_range = None
        
        # sales summary figures         
        self.revenue = 0
        self.units_sold = 0
        
        # utils
        self.orders_df = None
        self.generated_workbook = None  
        self.formatted_workbook = None
        self.report_name = None
                    
    def simple_sales_report(self, orders_df: pd.DataFrame, inventory_df: pd.DataFrame) \
        -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns a sales report with revenue/units sold, and remaining units, grouped by SKU/product 
        
        Parameters: 
        -orders_df: pd.DataFrame containing contents of 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL' report
        -inventory_df: pd.DataFrame containing contents of 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA' report
        
        Returns:
        -Tuple of pd.DataFrames, the first containing the finished report and the second containing raw orders data.        
        """

        if not isinstance(orders_df, pd.DataFrame) or not isinstance(inventory_df, pd.DataFrame):
            raise TypeError("Parameter inputs must be of Pandas DataFrame type")
        
        # break if 0 sales (crucial info is derived from the data - no point continuing if its missing)
        if orders_df.empty:
            logging.error(f"Passed an empty DataFrame for 'orders' - (only headers in df)")
            raise ZeroSalesError("No sales for this account/date range (only headers in df)")
 
        # break if df isnt empty but if sum of item-price is $0 (only removal orders, switcheroos, etc)
        if orders_df['item-price'].sum() == 0:
            logging.error(f"Passed an empty DataFrame for 'orders' - (item-price col sums to $0)")
            raise ZeroSalesError("No sales for this account/date range (item-price col sums to $0)")

        # not critical if 0 inventory, potentially just out-of-stock, can continue
        if inventory_df.empty:
            logging.info(f"Passed an empty DataFrame for 'inventory'")

        # validate report columns 
        required_columns = [
            (orders_df, ['sku', 'product-name', 'purchase-date', 'item-price', 'quantity']),
            (inventory_df, ['sku', 'afn-fulfillable-quantity'])
        ]

        for df, columns in required_columns:
            for column in columns:
                if column not in df.columns:
                    raise ValueError(f"Missing column '{column}' in your DataFrame")
                
        try:
            # pass orders_df in raw form before any adjustments are made (for raw data tab in finished report)
            self.orders_df = orders_df
            
            # get date range of report(purchase-date column is ISO str)
            orders_df['purchase-date-fmt'] = pd.to_datetime(
                orders_df['purchase-date'].str.split('T').str[0]
                ).dt.date
            self.date_start = orders_df['purchase-date-fmt'].min()
            self.date_end = orders_df['purchase-date-fmt'].max()
            self.date_range = (self.date_end - self.date_start).days

            # clean the value columns for orders
            for numeric_col in ['item-price', 'quantity']:
                orders_df[numeric_col] = pd.to_numeric(
                    orders_df[numeric_col].fillna(0),
                    errors='coerce'
                    )

            # group by sku/product, sum revenue/units sold
            orders_pivot = pd.pivot_table(
                orders_df.loc[(orders_df['item-price'] > 0)],
                index=['sku', 'product-name'],
                values=['quantity', 'item-price'],
                aggfunc='sum'
                ).reset_index()
            
            # clean the value columns for inventory
            inventory_df['afn-fulfillable-quantity'] = pd.to_numeric(
                inventory_df['afn-fulfillable-quantity'].fillna(0),
                errors='coerce'
                )
            
            # merge inventory data to the orders data
            final_df = pd.merge(
                orders_pivot,
                inventory_df[['sku', 'afn-fulfillable-quantity']],
                on='sku',
                how='left'
            )
            
            # just in case if the sku doesn't exist in the inventory report
            final_df['afn-fulfillable-quantity'] = final_df['afn-fulfillable-quantity'].fillna(0)
            
            # get totals 
            self.revenue += final_df['item-price'].sum()
            self.units_sold += final_df['quantity'].sum()
            
            # create a grand totals row 
            grand_totals_row = pd.DataFrame({
                'sku': ['Grand Total'],
                'product-name': [''],
                'quantity':[self.units_sold],
                'item-price':[self.revenue],
                'afn-fulfillable-quantity':[pd.NA] 
            })
            
            # add the new row to df
            final_df = pd.concat([final_df, grand_totals_row], ignore_index=True)
            
            # order by most-ordered products (brings the grand totals row to the top)
            final_df = final_df.sort_values(['quantity', 'product-name'], ascending=[0,1])

            # rename columns
            final_df = final_df.rename(columns={
                'quantity': 'units sold',
                'item-price': 'revenue',
                'afn-fulfillable-quantity': 'remaining units'
            })
            
            # reorder columns
            select_columns = ['sku', 'product-name', 'units sold', 'revenue', 'remaining units']
            final_df = final_df[select_columns]
            
            # thank you come again
            self.generated_workbook = final_df
            return self.generated_workbook, self.orders_df
        
        except Exception as e:
            logging.error(f"Error generating sales report, could not write orders/inventory to df: {str(e)}")
            raise

    def format_simple_sales_report(
        self, 
        df: Union[pd.DataFrame, io.BytesIO], 
        raw_df: Union[pd.DataFrame, io.BytesIO]
    ) -> io.BytesIO:
        """
        Uses openpyxl to visually format the df generated from `simple_sales_report`. Returns buffer object.
        
        Parameters:
            -df: Union[pd.DataFrame, io.BytesIO] - The main df with the pivot table sales results (Sheet 1)
            -raw_df: Union[pd.DataFrame, io.BytesIO] - The raw orders data (Sheet 2)
        """

        # validate user input and create an Excel workbook in memory 
        input_buffer = io.BytesIO()
        with pd.ExcelWriter(input_buffer, engine='openpyxl') as writer:
            for sheet_name, _df in [('Summary', df), ('Raw Data', raw_df)]:
                if isinstance(_df, pd.DataFrame):
                    _df.to_excel(writer, sheet_name=sheet_name, index=False)
                elif isinstance(_df, io.BytesIO):
                    io_to_df = pd.read_excel(_df)
                    io_to_df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    raise ValueError(f"Passed a non DataFrame or BytesIO object to the {sheet_name} sheet.")
        input_buffer.seek(0)

        # continue on to format the created workbook
        try:
            # load the df to xl workbook
            wb = xl.load_workbook(input_buffer) 
            ws = wb['Summary']  # only loading in 'Summary'. Since 'Raw_Data' wont be fmt'd, no need to load it in
            
            # instantiate a styler 
            styler = Style(ws)
            
            # create a table out of the array, and center/widen the rows
            styler.align_and_center()
            styler.create_table()
            
            # format certain cells with bold/highlight 
            # NOTE: this code is confusing to read, please refer to utils.py if anything
            cells_to_fmt = {'A2': [True, False], 'C2': [True, True], 'D2': [True, True]}
            for cell, fmt in cells_to_fmt.items():
                styler.apply_styles_to_cell(cell=cell, bold=fmt[0], highlighter=fmt[1])

            # change header text color to white, make it more legible
            for cell in ['A1', 'B1', 'C1', 'D1', 'E1']:
                styler.change_font_color(cell, "FFFFFFFF")

            # add currency and/or thousands separator to certain columns
            numeric_cols_fmt = {'C': False, 'E': False, 'D': True}
            for col, fmt in numeric_cols_fmt.items():
                styler.currency_formatter(col, currency=fmt)
            
            # save
            output_buffer = io.BytesIO()  # reset the initial buffer (corrupts otherwise)
            wb.save(output_buffer)
            output_buffer.seek(0)
            self.formatted_workbook = output_buffer   
            return self.formatted_workbook
        
        except Exception as e:
            logging.error(f"Unexpected error formatting/styling report: {str(e)}")
            raise

    def set_report_name(self) -> str:
        """Helper method for `simple_daily_sales`. Generates report_name (up to the user to save with it)"""
        
        if self.generated_workbook is None:
            raise ValueError("Must call `simple_daily_sales` first")
        
        try:
            # format the dates properly (month-day-year instead of day-month-year)
            start_date = self.date_start.strftime('%m-%d-%Y')
            end_date = self.date_end.strftime('%m-%d-%Y')
            
            # create report name based on account name, dates and sales figures
            if self.account_name is None:
                if self.date_range <= 1:
                    self.report_name = f"Sales {start_date} ({self.units_sold} units, ${self.revenue:,.2f})"
                else:
                    self.report_name = \
                        f"Sales {start_date} through {end_date} ({self.units_sold} units, ${self.revenue:,.2f})"
            
            elif self.account_name:
                if self.date_range <= 1:
                    self.report_name = \
                        f"{self.account_name.title()} Sales {start_date} ({self.units_sold} units, ${self.revenue:,.2f})"
                else:
                    self.report_name = (
                        f"{self.account_name.title()} Sales {start_date} through {end_date} "
                        f"({self.units_sold} units, ${self.revenue:,.2f})"
                    )
                            
            return self.report_name

        except Exception as e:
            logging.error(f"Could not set the report name: {str(e)}")
            raise
