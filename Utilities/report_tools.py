from ast import literal_eval
from datetime import datetime, timedelta
import gzip
import io
import json
import logging
import os
import re
from typing import Optional, Tuple, Union

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

import openpyxl as xl
from openpyxl.worksheet.worksheet import Worksheet
import pandas as pd
import pytz
import requests as req

from Utilities.utils import Helpers, Style


class ZeroSalesError(Exception):
    pass


class GenerateFBAReport:
    """Downloads data from the Amazon Reports SP-API

    Requirements: 
        (1). AZ Key Vault(s) with SP-API keys: [client secret, client id, refresh token, rotation deadline] 

        (2). The following Environment Variables created in your Function App settings:
            -ACCOUNTS_LIST: Accounts initials, separated by commas, for which you will be running the reports 
            -<ACCOUNT-INITIALS>_VAULT_NAME: The name of the Key Vault for each account
            -CLIENT_ID: The key name of the 'client id' secret
            -CLIENT_SECRET: The key name of the 'client secret' secret
            -REFRESH_TOKEN: The key name of the 'refresh token' secret
            -ROTATION_DEADLINE: the key name of the 'rotation deadline' secret
            -MARKETPLACE_ID: 'ATVPDKIKX0DER' (For North America)
            -ENDPOINT: 'https://sellingpartnerapi-na.amazon.com/reports/2021-06-30'
            -TOKEN_REQUEST_URL: 'https://api.amazon.com/auth/o2/token'


    Example: 
        (a). For account "Platinum Oversized", create a KV "po-kv"
        (b). Add the SP-API keys specified in (1) to this new KV as secrets          
        (c). In your Function App, create the env-variables specified in (2)
        (d). If you have several accounts, create a separate KV for each, and add acc initials to ACCOUNTS_LIST

        In this scenario, modify ACCOUNTS_LIST to include 'PO' (e.g. ['PO','ACC1','ACC2'])
        If you only have one account, ACCOUNTS_LIST must still be a list (e.g. ['PO'])
        Then, create an env-var "PO_VAULT_NAME" - its value would be the name of the KV ("po-kv")
        Next, pass the SPI-API keys from (1) as Function App env-variables - title them exactly as shown in (2)
        For example, within KV "po-kv" create secret name "po-client-secret" with secret value "ABCD1234"
        Then, in the Function App, create an env-variable "CLIENT_SECRET" with value "po-client-secret"
        Repeat for 'CLIENT_SECRET', 'REFRESH_TOKEN' and 'ROTATION_DEADLINE'
        
        (TL;DR - the environment variables point to the key-names, which point to the secret values)

        'MARKETPLACE_ID', 'ENDPOINT', 'TOKEN_REQUEST_URL' can be simply entered as env-variables without KV
             
             
    Considerations:            
        -Maximum date range for any report in this API is 31 days. For longer ranges, run in loops
        
        -This class uses `DefaultAzureCredential` authentication, so ensure your managed identities are in order

        -Eager-validates environment variables and keys, so ensure the above above requirements are all met

        -Full list of available reports to generate using this class: 
        https://developer-docs.amazon.com/sp-api/docs/report-type-values-fba    
    """
    def __init__(self):    
        # validating current accounts list
        try:
            self.current_accounts = literal_eval(os.getenv('ACCOUNTS_LIST'))
        except SyntaxError:
            raise SyntaxError("No accounts detected in ACCOUNTS_LIST env-var. Pass a list with at least one acc name")

        if isinstance(self.current_accounts, list) and len(self.current_accounts) == 0:
            raise SyntaxError("Empty list detected for ACCOUNTS_LIST. Please pass at least one acc name")
        
        # validating environment variables 
        self.__validate_environment_variables()

        # validating date range input (populates during `request_FBA_report` via `__validate_user_input`)
        self.start_date_iso, self.end_date_iso = None, None        
        
        # vault and api keys
        self.key_vault = None
        self.client_secret = None
        self.refresh_token = None
        self.client_id = None  
        self.rotation_deadline = None

        # utils and general attributes
        self.backoff = Helpers()
        self.reports_url = os.getenv("ENDPOINT")
        self.access_token = None
        self.report_id = None 
        self.report_endpoint = None
        self.report_type = None 
        self.download_url = None
        self.compression = None
    
    def __validate_environment_variables(self) -> None:
        """Private method: validates the Function App environmental variables upon class instantiation"""

        # vault name environment variables per each account
        account_env_vars_required = [f"{account.upper()}_VAULT_NAME" for account in self.current_accounts]
        
        # general environment variables 
        general_env_vars_required = [
            "ACCOUNTS_LIST",
            "CLIENT_ID",
            "CLIENT_SECRET",
            "REFRESH_TOKEN",
            "TOKEN_REQUEST_URL",
            "MARKETPLACE_ID",
            "ENDPOINT",
            "ROTATION_DEADLINE"
        ]
        
        required_env_vars = general_env_vars_required + account_env_vars_required

        # break program if any env vars are missing
        missing_vars = [var for var in required_env_vars if not os.getenv(var) or len(os.getenv(var)) == 0]        
 
        if missing_vars:
            missing_vars = ', '.join(missing_vars)
            logging.error(f"Missing the following environmental variables: {missing_vars}")
            raise ValueError(
                f"""Missing some environmental vars: check your 'ACCOUNTS_LIST' and '<name>_VAULT_NAME' vars. 
                Missing: {missing_vars}"""
                )
        else:
            logging.info("Successfully validated all required environment variables")
         
    def __validate_user_input(self, start_date: str, end_date: str) -> Tuple[str, str]:
        """Private method: validates ['start_date', 'end_date'] inputs to `request_fba_report` method"""
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
            
            # assign to instance attributes 
            self.start_date_iso = start_date_iso
            self.end_date_iso = end_date_iso

            return start_date_iso, end_date_iso

        except Exception as e:
            logging.error(f"Could not validate your date parameters: {str(e)}")
            raise
    
    def _init_key_vault(self, account_name: str) -> SecretClient:
        """Returns SecretClient object for the account initials specified, which allows access to secrets
        
        Parameters:
            -account_name: (str) The account name initials of the key vault you wish to initialize (e.g. "PO")
        """
        vault_name = os.getenv(f'{account_name.upper()}_VAULT_NAME')
        if not vault_name or len(vault_name) == 0:
            raise ValueError(f'Could not locate vault name environmental variable for account: {account_name}')
        
        try:             
            secret_client = SecretClient(
                vault_url=f"https://{vault_name}.vault.azure.net",
                credential=DefaultAzureCredential()
            )
            
            # populate attribute
            self.key_vault = secret_client
            
            logging.info("Successfully initialized and accessed the Key Vault")                
            return secret_client 
       
        except Exception as e:
            logging.error(f"Could not initialize the key vault client.{str(e)}")
            raise

    def _validate_key_vault(self) -> None:
        """Private method: validates the existence of the required keys in the Key Vault"""
        
        if self.key_vault is None:
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
            raise ValueError(f"Missing some keys from Key Vault")
        else:
            logging.info("Successfully fetched and validated all required keys")

    def _fetch_from_key_vault(self, key_name: str) -> str:
        """
        Retrieves the secret value for a key name, from the vault specified by the 'x_vault_name' environment variable
                
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

    def get_amz_keys(self, account_name: str) -> None:
        """
        Populates client_id, client_secret, rotation_deadline and refresh_token instance attributes 
        for an acccount, enables access to SP-API"""

        # initialize the key vault 
        if not self.key_vault:
            self._init_key_vault(account_name=account_name)
        
        # validate the keys and env vars before proceeding
        self._validate_key_vault()
                
        keys_dict = {
            'client_id': os.getenv("CLIENT_ID"),
            'client_secret': os.getenv("CLIENT_SECRET"),
            'refresh_token': os.getenv("REFRESH_TOKEN"),
            'rotation_deadline': os.getenv("ROTATION_DEADLINE")
        }
        
        # populate instance attributes with the key secrets
        for k, v in keys_dict.items():
            try:
                secret_value = self._fetch_from_key_vault(v) 
                setattr(self, k, secret_value)
                logging.debug(f"Successfully fetched key for {k}")   

            # break program if any key names are missing                                  
            except Exception as e:
                logging.error(f"Could not fetch key for {k}: {str(e)}")
                raise

        logging.info("Successfully fetched all keys from vault")
        
        # terminate if SP-API keys expired (will probably get error regardless but double-protection FTW)
        key_expiration_date = datetime.strptime(self.rotation_deadline, "%Y-%m-%d").date()
        if datetime.today().date() >= key_expiration_date:
            raise ValueError(
                "Your API keys have expired. Please generate new ones via the SellerCentral portal"
                )

    def request_access_token(self) -> str:
        """Requests LWA access token from the SP-API, and returns the token as a string"""

        if not any([self.client_id, self.client_secret, self.refresh_token]):
            logging.error("Must first get the key vault secrets before requesting an access token")
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
                    logging.debug("Successfully fetched request token")
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
    
    def request_FBA_report(
        self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None, 
        report_type: Optional[str] = None
    ) -> str:
        """
        Requests a downloadable report from the SP-API reports endpoint
        
        Parameters:
            start_date: Optional[str]: The start date range you wish to run the report for. Default=None (yesterday)
            end_date: Optional[str] The ending date range you wish to run the report for. Default=None (today)
            report_type Optional[str]: Default = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'

        Returns:
            report_id (str) The report_id of the report you requested
            Also populates report_id instance attribute
        """
        # get LWA token if it hasn't been run yet 
        if self.access_token is None:
            logging.warning('No access token attribute detected. Requesting one now')
            self.request_access_token()
                
        # default to orders report if param is left blank
        if not report_type:
            report_type = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'

        # validate the start/end date ranges (populates the start/end date iso attributes)
        self.__validate_user_input(start_date=start_date, end_date=end_date)
        logging.info(f"Proceeding with report date range: {self.start_date_iso} - {self.end_date_iso}")

        # set instance variable         
        self.report_type = report_type

        # inv report doesn't take date params, but they dont break it either
        report_params = {
            'marketplaceIds': [os.getenv("MARKETPLACE_ID")],
            'reportType': self.report_type,
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
            logging(f"Maximum allotted retries reached, could not request report '{self.report_type}'")
            raise ValueError(f"Maximum allotted retries reached: could not request report '{self.report_type}'")
        else:
            self.report_endpoint = self.reports_url + f"/reports/{self.report_id}"

    def check_report_status(self, report_id: Optional[str] = None) -> str:
        """
        Returns status of the report_id passed from instance attribute or parameter 
        
        Parameters:
            report_id (Optional[str]): Report ID for which to check the status
            Default = current report_id instance attribute
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
                logging.info(f"Report ID {self.report_id} - '{self.report_type}' - Status: '{status}'")
                
            return status
        
        except Exception as e:
            logging.exception(f'Unexpected error occurred trying to get report status {str(e)}')
            status = 'N/A'
            return status

    def check_requested_reports(self, report_type: Optional[str] = None) -> str:
        """
        View reports available for download for the specified report type for your account

        Parameters-
            -report_type: Report name you wish to check the status of (ex: 'GET_FLAT_FILE_ORDERS_DATA')
            Default = report_type instance attribute 
        
        Returns:
            -dump of raw str JSON with all of the requested reports, incl. Next Tokens
        """
        if report_type is None:
            report_type = self.report_type
        
        if report_type is None and self.report_type is None:
            raise ValueError("'report_type' not provided, please enter a parameter or run `request_FBA_report` first")       

        if self.access_token is None:
            raise ValueError("No access token located. Need to run the `request_access_token` method first")

        headers = {
            'x-amz-access-token': self.access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        params = {
            "reportTypes": {report_type}
        }

        get_status = req.get(
            url=self.reports_url + '/reports',
            headers=headers,
            params=params
        )
        
        if get_status.status_code != 200:
            logging.error(f"Could not check requested reports, status code: {get_status.status_code}")

        return json.dumps(
                get_status.json(),
                indent=4
                )
    
    def get_last_ready_report_id(self, report_type: Optional[str] = None) -> str:
        """
        Retrieves the most recent 'DONE' status report for the report_type specified, under your account

        In the case of a 'FATAL' error during a request, this can be placed in the 'ELSE' block to download
        the most recent successfully generated report instead, as a backup. 
        
        Parameter: 
            -report_type: the name of the report you wish to retrieve the report ID of
            
        Returns:
            -report_id (also updates the current report_id attribute, so you can pass it down to the download method)

        """
        if report_type is None:
            report_type = self.report_type
        
        if report_type is None and self.report_type is None:
            raise ValueError("No report_type found. Enter a report_type parameter or run `request_FBA_report` first")
        #  get json dump of existing reports for the report type, convert to df 
        try:
            reports_dump = self.check_requested_reports(report_type=report_type)
            requested_reports_json = json.loads(reports_dump).get('reports', '')
       
        except Exception as e:
            logging.error(
                f"Could not fetch json of 'recently generated reports' for report type {report_type}: {str(e)}"
                )
            raise
            
        try:
            # convert to df for easy filtering
            requested_reports_df = pd.DataFrame(requested_reports_json)
            
            # format col as datetime to find the most recent date
            requested_reports_df['processingEndTime'] = \
                pd.to_datetime(requested_reports_df['processingEndTime'], errors='coerce')
            
            # filter for ready reports only            
            requested_reports_df = requested_reports_df.loc[requested_reports_df['processingStatus'] == 'DONE']

            # find most recent 'DONE' report, and return the corresponding report id for it
            if not requested_reports_df.empty:
                latest_report_date = requested_reports_df['processingEndTime'].max()

                most_recent_completed_report_id = \
                    requested_reports_df.loc[requested_reports_df['processingEndTime'] == latest_report_date]
                    
                most_recent_completed_report_id = str(
                    most_recent_completed_report_id['reportId'].iloc[0]
                    )
                
                self.report_id = most_recent_completed_report_id
                return most_recent_completed_report_id
        
            else:
                logging.error(f"Could not locate any 'DONE' reports for report type {report_type}")
                raise ValueError(f"Could not locate any reports to fall back to for report type {report_type}")

        except Exception as e:
            logging.error(f"Could not parse the 'recently generated reports' df to retrieve a report ID: {str(e)}")
            raise

    def get_download_url(self, report_id: Optional[str] = None) -> Tuple[str, str]:
        """
        Fetches the download_url for the report_id, assuming report status is 'DONE'
        
        Parameters:
            report_id: Optional[str] - Report id you wish to get download URL for
            If left blank, will default to the current report_id instance attribute and
            populates the compression and download_url attributes
            If not left blank, updates the instance attributes with the passed values, so that 
            you can use 'get_download_url' and 'download_report' immediately after without passing any params
    
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
            # if not report_id:
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
            raise ValueError("Must provide a compression type - if there is none, enter 'No compression'")
        
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
                    logging.debug(f"Download prepared, now decompressing and writing to df")
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
            

class ReportAssembler:
    """Compiles and styles/formats DataFrames and IO objects, into .xlsx files/reports
    
    Parameters:
        -account_name: (Optional[str]) adds name to the report title
    """
    
    def __init__(self, account_name: Optional[str] = None):
                  
        # these will be used to define the report name 
        self.account_name = account_name        
        self.date_start = None
        self.date_end = None
        self.date_range = None
        self.today = datetime.now().date().strftime("%m-%d-%Y")
        
        # sales summary figures         
        self.revenue = 0
        self.units_sold = 0
        
        # utils
        self.Helpers = Helpers()
        self.orders_df = None
        self.generated_workbook = None  
        self.formatted_workbook = None
        self.report_name = None
                    
    def simple_sales_report(
        self, 
        orders_df: pd.DataFrame,
        inventory_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Returns a sales report with revenue/units sold, and remaining units, grouped by SKU/product 
        
        Parameters: 
            -orders_df: DataFrame containing contents of 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL' report
            -inventory_df: DataFrame containing contents of 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA' report
        
        Returns:
            -Tuple of pd.DataFrames containing the finished report in index 0, and the raw order data in index 1        
        """

        if not isinstance(orders_df, pd.DataFrame) or not isinstance(inventory_df, pd.DataFrame):
            raise TypeError("Parameter inputs must both be of Pandas DataFrame type")
        
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
        
        # assign the raw orders_df (for Sheet2) before any editing  
        self.orders_df = orders_df
        
        try:
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
            (Parameters can be Pandas DataFrames or IO Bytes objects)
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
            
            # format certain cells with bold/highlight (confusing to read, refer to utils.py, sorry)
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
            
            # add data bars to numeric columns (easier to interpret)
            for column in ['C', 'D', 'E']:
                styler.data_bars(column=column, start_row=3)  # avoid grand totals row
            
            # save
            output_buffer = io.BytesIO()  # reset the initial buffer (corrupts otherwise)
            wb.save(output_buffer)
            output_buffer.seek(0)
            self.formatted_workbook = output_buffer   
            return self.formatted_workbook
        
        except Exception as e:
            logging.error(f"Unexpected error formatting/styling report: {str(e)}")
            raise

    def set_simple_sales_report_name(self) -> str:
        """Helper method for `simple_daily_sales`. Generates report_name (up to the user to then save with it)"""
        
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
                        f"{self.account_name.title()} Sales {start_date} ({self.units_sold} units,${self.revenue:,.2f})"
                else:
                    self.report_name = (
                        f"{self.account_name.title()} Sales {start_date} through {end_date} "
                        f"({self.units_sold} units, ${self.revenue:,.2f})"
                    )
                            
            return self.report_name

        except Exception as e:
            logging.error(f"Could not set the report name: {str(e)}")
            raise

    def on_hand_report_compiler(self, orders: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
        """
        Takes the concat'd orders and inventory df's and pivots them into a raw on-hand report
        
        Parameters:
            -orders: (pd.DataFrame) df of the 90D (or other date range) orders data
            -inventory: (pd.DataFrame) df of the current inventory data
        
        Returns: 
            -Pandas DataFrame with the on-hand report 
            columns=['SKU', 'ASIN', 'PRODUCT NAME', 'BRAND', 'ON HAND', 'RECEIVED']
        """
        # basic validation to make sure inputs are correct 
        if not any([isinstance(orders, pd.DataFrame), isinstance(inventory, pd.DataFrame)]):
            raise TypeError("Report compiler only accepts Pandas dfs - convert your order/inv data to df first")
        
        required_order_columns = ['sku', 'quantity']
        for column in required_order_columns:
            if column not in orders.columns:
                raise KeyError(f"Required column {column} not found in your orders df")
                    
        required_inventory_columns = ['sku', 'asin', 'product-name', 'afn-fulfillable-quantity']
        for column in required_inventory_columns:
            if column not in inventory.columns:
                raise KeyError(f"Required column {column} not found in your inventory df")
        
        # proceed with report generation
        try:            
            # pivot orders table
            orders = orders.groupby('sku').agg({'quantity':'sum'}).reset_index()
            
            # merge to inventory df
            final_df = pd.merge(inventory, orders, on='sku', how='left')
            
            # left join will inevitably lead to blanks in the orders.quantity column
            final_df['quantity'] = final_df['quantity'].fillna(0)
            
            # add received col
            final_df['received'] = final_df['afn-fulfillable-quantity'] + final_df['quantity']
            
            # filter out items we have not received 
            final_df = final_df.loc[final_df['received'] > 0]
            
            # remove redundant cols
            final_df = final_df[['sku', 'asin', 'product-name', 'afn-fulfillable-quantity', 'quantity', 'received']]
            
            # rename cols
            final_df.rename(columns={'afn-fulfillable-quantity': 'on-hand'}, inplace=True)
            
            # drop quantity column as its no longer needed
            final_df.drop('quantity', axis=1, inplace=True)
            
            # sort by in-stock items
            final_df.sort_values('on-hand', ascending=0, inplace=True)
            
            return final_df        
        
        except Exception as e:
            logging.error(f"Failure compiling the orders/inv dfs in report_compilter(): {str(e)}")
            raise
    
    def on_hand_report_formatter(self, ws: Worksheet, table_name: str = 'Table1') -> None:
        """
        Formats the on-hand report created in `on_hand_report_compiler()` method with openpyxl 

        Parameters:
            -ws (openpyxl.Worksheet): The sheet that you wish to format
            -table_name (str): The data array will be transformed into a Table (default='Table1')
            
        Considerations:
            -If your report contains multiple accounts, pass a distinct table name for each sheet, as Excel
            only allows unique names for Tables, even if they're on different tabs
            
            -Worksheet is not saved after formatting, you must save/close Workbook after running this method
        """
        # basic validation
        if not isinstance(ws, Worksheet):
            raise TypeError("The input parameter must be an openpyxl Worksheet")
        
        # init styler util   
        pen = Style(ws)
        
        # center, align, create table out of array/range 
        pen.align_and_center()
        pen.create_table(table_name=table_name)
        
        # change header text to white 
        for cell in ['A1', 'B1', 'C1', 'D1', 'E1']:
            pen.change_font_color(cell=cell, color="FFFFFFFF")

        # add data bars         
        for col in ['D', 'E']:
            pen.data_bars(column=col)
        
        # exit
        return None

    def set_on_hand_report_name(self):
        """Sets the on hand report name, using the account initials and date the report was ran"""

        if self.account_name is None:
            logging.warning("Account name left blank, on hand report account is ambiguous")
            self.report_name = f"On Hand {self.today}"
            return self.report_name
        else:
            self.report_name = f"{self.account_name.upper()} On Hand {self.today}"
            return self.report_name

class ReportDownloadOrchestrator:
    """
    Helper class to more easily generate downloadable reports from SP-API, using the `GenerateFBAReport` class
    
    Parameters:
        -account_name: (str) The account initials you wish to generate the report for 
    
    Considerations:
        -Note the requirements for `GenerateFBAReport` class (refer to its docstring)      
    """
    def __init__(self, account_name: str):       
        self.account_name = account_name
        
        # eager load the required classes
        self.GenerateFBAReport = GenerateFBAReport()
        self.Helpers = Helpers() 
        
        # get API keys, but catch any issues that may arise with account parameters
        try:
            self.GenerateFBAReport.get_amz_keys(account_name=self.account_name)
        except Exception as e:
            logging.error(
                f"""Could not fetch API keys for '{account_name}' during report orchestration. 
                Make sure the name exactly matches your environment-variables and key vault. For example, if your
                FBA Seller Account is 'Test Seller', make sure you pass initials 'TS' to the account_name parameter, 
                that your key vault is titled 'ts-kv', and that the env-variable is 'TS_VAULT_NAME'"""
            )
            raise
        
        # get access token once, so you needn't request it each time
        self.GenerateFBAReport.request_access_token()
    
    # common date ranges as properties for easy access (TODO: add more later as they become necessary) 
    @property
    def today(self):
        today_date = datetime.now().date()
        today_str = today_date.strftime("%m-%d-%Y") 
        return today_str
    
    @property
    def one_month_ago(self):
        today_date = datetime.now().date()
        one_month_ago_date = today_date - timedelta(days=30)
        one_month_ago_str = one_month_ago_date.strftime("%m-%d-%Y")
        return one_month_ago_str
    
    @property
    def two_months_ago(self):
        today_date = datetime.now().date()
        two_month_ago_date = today_date - timedelta(days=60)
        two_month_ago_str = two_month_ago_date.strftime("%m-%d-%Y")
        return two_month_ago_str

    @property
    def three_months_ago(self):
        today_date = datetime.now().date()
        three_month_ago_date = today_date - timedelta(days=90)
        three_month_ago_str = three_month_ago_date.strftime("%m-%d-%Y")
        return three_month_ago_str
        
    def get_report(self, report_type: str, start_date: str, end_date: str) -> str:
        """
        Requests orders by date range from Amazon SP-API (Requests, waits until ready, and downloads)
        
        Parameters:
            -report_type: (str) The name of the SP-API you wish to generate/download
            -start_date: (str) The starting date of the range you wish to run the report for
            -end_date: (str) The ending date of the range you wish to run the report for 
        
        Returns:
            -str: report contents in json, so as to be transferable between durable functions
 
        Considerations:
            -Refer to 'GenerateFBAReport' class docstrings for specificities about possible parameters   
            -You can pass dates to `get_report` method, or use class properties containing some common date-ranges         
        """
        
        # request the report using the class input parameters 
        self.GenerateFBAReport.request_FBA_report(
            report_type=report_type,
            start_date=start_date,
            end_date=end_date
        )
        
        # check report status and download once ready 
        df = None
        current_attempt = 1
        max_attempts = 7
        while current_attempt <= max_attempts:
            try:
                status = self.GenerateFBAReport.check_report_status()
                
                if status == 'DONE':
                    self.GenerateFBAReport.get_download_url()
                    df = self.GenerateFBAReport.download_report()
                    df = df.to_json(orient='records')
                    return df
                
                elif status in ['FATAL', 'CANCELLED']:
                    logging.warning(f"Status: {status} for {report_type}")
                    # if ORDER report fails, must break, as the date ranges are uncertain for existing reports
                    # INVENTORY reports, however, have no date range so we can default to the most recent report
                    # they generate every 30 min anyway, near real time data
                    # TODO: must list all reports that dont require a date range, just doing unsupressed inv for now
                    if report_type == 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA':
                        logging.info("Falling back to most recent available inventory report")
                        self.GenerateFBAReport.get_last_ready_report_id(report_type=report_type)
                        self.GenerateFBAReport.get_download_url()
                        df = self.GenerateFBAReport.download_report()
                        df = df.to_json(orient='records')
                        return df                        

                    # if order report, and not inventory, break
                    else:
                        raise RuntimeError(f"Couldn't get orders for {start_date}-{end_date}, {max_attempts} attempts")
                
                else:
                    # added longer timer here because SP-API is sensitive
                    self.Helpers.exponential_backoff(n=current_attempt, base_seconds=10, rate_of_growth=1.75)
                    current_attempt += 1
                    
            except Exception as e:
                logging.error(f"Error on attempt {current_attempt}: {str(e)}")
                self.Helpers.exponential_backoff(n=current_attempt, base_seconds=10, rate_of_growth=1.75)
                current_attempt += 1
        
        # break if couldn't populate df after max attempts
        if df is None:
            raise RuntimeError(f"Couldn't fetch orders for range {start_date}-{end_date} after max attempts")