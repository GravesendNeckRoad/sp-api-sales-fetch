# _____________________________________________________________________________________________________________________
#                                               FOR INTERNAL USE ONLY
#                                               [REDACTED] TRADING CO.
# _____________________________________________________________________________________________________________________

from datetime import datetime, timedelta
from email.message import EmailMessage
import gzip
import io
from json import dumps
import logging
import os
import re
import ssl
from smtplib import SMTP_SSL
from typing import List, Optional, Tuple, Union

import openpyxl as xl
import pandas as pd
import pytz
import requests as req

import config
from utils import Helpers, Style

pd.set_option('display.width', None)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


class SalesReport:
    """
    Creates sales report from API by sku/product-name, with columns for units/revenue sold & remaining units in stock.

    :param start_date (str): The desired starting date of the report.
    :param end_date (str): The desired ending date range.

    *See config.py for all parameters/credentials required to run this class.
    *Leaving both parameters blank will default to a 24h sales report from the moment this class was instantiated.
    """
    def __init__(self, start_date: str = None, end_date: str = None):
        self.start_date_iso, \
            self.end_date_iso, \
            self.start_date, \
            self.end_date, \
            self.report_length_in_days \
            = self.__validate_user_input(start_date, end_date)

        self.start_date_str = self.start_date.strftime("%m-%d-%Y")
        self.end_date_str = self.end_date.strftime("%m-%d-%Y")

        self.access_token = self.__request_access_token()
        self.report_name = None
        self.output_path = None
        self.write_to_disk = None

        self.total_revenue = 0
        self.total_units = 0

        self.zero_sales = False
        self.zero_inventory = False

        self.helper = Helpers()

    @staticmethod
    def __validate_user_input(start_date: str, end_date: str) -> Tuple[str, str, datetime, datetime, int]:
        """Validates the 'start_date' and 'end_date' constructor parameters passed by user."""
        try:
            key_expiration_date = datetime.strptime(config.credentials['rotation_deadline'], "%Y-%m-%d").date()
            if datetime.today().date() >= key_expiration_date:
                raise ValueError("Your API keys have expired. Please generate new ones via the SellerCentral portal.")

            today = datetime.now(pytz.timezone('US/Eastern')).astimezone(pytz.utc)
            yesterday = (today - timedelta(days=1))

            if start_date is None:
                start_date_formatted = yesterday
            else:
                start_date_cleaned = re.sub(r"[/.]", "-", start_date)
                start_date_formatted = datetime.strptime(start_date_cleaned, '%m-%d-%Y')
                start_date_formatted = pytz.timezone('US/Eastern').localize(start_date_formatted)

            if end_date is None:
                end_date_formatted = today
            else:
                end_date_cleaned = re.sub(r"[/.]", "-", end_date)
                end_date_formatted = datetime.strptime(end_date_cleaned, '%m-%d-%Y')
                end_date_formatted = pytz.timezone('US/Eastern').localize(end_date_formatted)

            if start_date_formatted >= end_date_formatted:
                raise ValueError("The start_date must be a date before the end_date.")

            if end_date_formatted > today:
                raise ValueError("The end date cannot be later than today's date.")

            report_length_in_days = (end_date_formatted - start_date_formatted).days

            if report_length_in_days > 31:
                raise ValueError("The reports API can only generate a date range of up to 31 days long.")

            start_date_iso = start_date_formatted.isoformat()
            end_date_iso = end_date_formatted.isoformat()

            return start_date_iso, end_date_iso, start_date_formatted, end_date_formatted, report_length_in_days

        except ValueError as v:
            logging.error(f"{v}")
            raise ValueError(f"Could not validate your parameter inputs:\n{v}")

    def __request_access_token(self) -> str:
        """Requests access token from Amazon SP-API."""
        access_token = None
        retry_attempt = 1
        maximum_attempts = 5

        while retry_attempt < maximum_attempts:

            request_token = req.post(
                url=config.token_request_url,
                data={
                    'grant_type': 'refresh_token',
                    'refresh_token': config.credentials['refresh_token'],
                    'client_id': config.credentials['client_identifier'],
                    'client_secret': config.credentials['client_secret']
                })

            if request_token.status_code == 200:
                print(f"Successfully obtained request token. Status code: {request_token.status_code} ...\n")
                access_token = request_token.json().get('access_token')
                break

            else:
                try:
                    print(request_token.json())
                except ValueError:
                    print(request_token.text)
                logging.error(f"Failed to fetch request token. Status code: {request_token.status_code}")
                self.helper.exponential_backoff(retry_attempt)
                retry_attempt += 1

        if access_token is None:
            raise Exception(f"Failed to retrieve request token after {retry_attempt} attempts.")

        return access_token

    def get_reports_status(self, report_name: str) -> None:
        """
        View reports available for upload for your developer account

        :param report_name: The name of the report you wish to check status of. (ex: 'GET_FLAT_FILE_ORDERS_DATA')
        """

        headers = {
            'x-amz-access-token': self.access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        params = {
            "reportTypes": {report_name}
        }

        get_status = req.get(
            url=f"{config.endpoint}reports/2021-06-30/reports",
            headers=headers,
            params=params
        )

        print(
            dumps(
                get_status.json(),
                indent=4
            )
        )

    def get_inventory_data(self, report_type: str = None) -> pd.DataFrame:
        """
        Requests an inventory report from SP-API and returns a df with the contents.

        :param report_type: (str) The name of the SP-API report you want to request. (Default = 'FBA Manage Inventory')
        :return Pandas DataFrame with the raw contents of the generated report

        *This method supports several other inventory reports:
        https://developer-docs.amazon.com/sp-api/docs/report-type-values-fba#fba-manage-inventory-health-report

        *As of 09/2024, inventory reports can only be downloaded once every 30 min. Use sporadically to avoid throttle.
        """

        if not report_type:
            report_type = 'GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA'

        url = config.endpoint + "reports/2021-06-30/reports"

        request_params = {
            'marketplaceIds': [config.marketplace_id],
            'reportType': report_type
        }

        # ____________________________________________________________________________________________________________
        # SUBMIT REQUEST FOR REPORT
        # ____________________________________________________________________________________________________________

        report_request_attempts = 1
        max_attempts = 10
        report_url = None

        print(f"Requesting report: {report_type} ...\n")
        while report_request_attempts < max_attempts:

            request_fba_inventory = req.post(
                url=url,
                headers={'x-amz-access-token': self.access_token},
                json=request_params
            )

            if request_fba_inventory.status_code == 202:
                print("Report successfully requested. Now generating ...\n")
                report_id = request_fba_inventory.json().get('reportId')
                report_url = url + f"/{report_id}"
                break

            else:
                logging.error(f"Could not request inventory report. Error code {request_fba_inventory.status_code}.\n")
                self.helper.exponential_backoff(report_request_attempts)
                report_request_attempts += 1

        if report_url is None:
            raise Exception(f"Critical error occurred: Could not request the inventory report. \n{Exception}")

        # ____________________________________________________________________________________________________________
        # WAIT FOR IT TO GENERATE, THEN DOWNLOAD
        # ____________________________________________________________________________________________________________

        n_status_checks = 1
        max_status_checks = 10
        document_id = None

        while n_status_checks < max_status_checks:

            check_status = req.get(report_url, headers={'x-amz-access-token': self.access_token})
            status = check_status.json().get('processingStatus')
            print(f"Report status: {status}")

            if check_status.status_code == 200:
                if status == "DONE":
                    document_id = check_status.json().get('reportDocumentId')
                    print(f'Report is ready for download: "{document_id}"\n')
                    break

                elif status == 'FATAL' or status == 'CANCELLED':
                    raise Exception(f"Critical Error: Report status returned '{check_status.status_code}' ...")

                else:
                    self.helper.exponential_backoff(base_seconds=25, rate_of_growth=1.75, n=n_status_checks)
                    n_status_checks += 1

            else:
                logging.error(f"Could not request report. Error code {check_status.status_code}.")
                self.helper.exponential_backoff(n_status_checks)
                n_status_checks += 1

        if document_id is None:
            raise Exception("Could not download the report even after >30 minutes.")

        document_url = f"{config.endpoint}reports/2021-06-30/documents/{document_id}"
        report_dl_request = req.get(document_url, headers={'x-amz-access-token': self.access_token})
        download_link = report_dl_request.json().get('url')
        request_download = req.get(download_link, stream=True)
        report_contents = request_download.text

        # ____________________________________________________________________________________________________________
        # CONVERT TO DATAFRAME
        # ____________________________________________________________________________________________________________

        try:
            compression = report_dl_request.json().get('compressionAlgorithm', '')
            if compression == 'GZIP':
                report_contents = request_download.content

                with gzip.GzipFile(fileobj=io.BytesIO(report_contents)) as gz:
                    report_contents = gz.read().decode('latin1')

            in_stock_df = pd.read_csv(io.StringIO(report_contents), sep='\t', encoding='latin1')
            print('Successfully retrieved inventory report API data and converted to DataFrame.')
            print("_"*200)
            return in_stock_df

        except Exception as e:
            logging.error(f"Critical error occurred while converting in-stock inventory to df. \n{e}\n")

    def get_orders_data(self, report_type: str = None) -> pd.DataFrame:
        """
        Requests an orders report from SP-API and returns a df of the contents.

        :param report_type: (str) The name of the SP-API report you want to request. (Default = 'All Orders')
        :return Pandas DataFrame with the raw contents of the generated report

        *Can be used with several other reports:
        https://developer-docs.amazon.com/sp-api/docs/report-type-values-order
        """
        if not report_type:
            report_type = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'

        url = config.endpoint + "reports/2021-06-30/reports"

        request_params = {
            'marketplaceIds': [config.marketplace_id],
            'reportType': report_type,
            'dataStartTime': self.start_date_iso,
            'dataEndTime': self.end_date_iso,
            "reportOptions": {"fulfillmentChannel": "AFN"}
        }

        # ____________________________________________________________________________________________________________
        # SUBMIT REQUEST FOR REPORT
        # ____________________________________________________________________________________________________________

        report_request_attempts = 1
        max_attempts = 10
        report_url = None

        print(f"Requesting report: '{report_type}'. Date-range: {self.start_date} through {self.end_date} ...\n")
        while report_request_attempts < max_attempts:

            request_report = req.post(
                url=url,
                headers={'x-amz-access-token': self.access_token},
                json=request_params
            )

            if request_report.status_code == 202:
                print("Report successfully requested. Now generating ...\n")
                report_id = request_report.json().get('reportId')
                report_url = url + f"/{report_id}"
                break

            elif request_report.status_code == 400:
                raise Exception("Malformed request syntax. This report may not exist. Check your inputs and try again.")

            else:
                logging.error(f"Could not request report. Error code {request_report.status_code}.")
                self.helper.exponential_backoff(report_request_attempts)
                report_request_attempts += 1

        if report_url is None:
            raise Exception(f"Critical error occurred: Could not request the report. \n{Exception}\n")

        # ____________________________________________________________________________________________________________
        # WAIT FOR IT TO GENERATE, THEN DOWNLOAD
        # ____________________________________________________________________________________________________________

        n_status_checks = 1
        max_status_checks = 10
        document_id = None

        while n_status_checks < max_status_checks:
            check_status = req.get(report_url, headers={'x-amz-access-token': self.access_token})
            status = check_status.json().get('processingStatus')
            print(f"Report status: {status}")

            if check_status.status_code == 200:
                if status == "DONE":
                    document_id = check_status.json().get('reportDocumentId')
                    print(f'\nReport is ready for download at: "{document_id}"\n')
                    break

                elif status == 'FATAL' or status == 'CANCELLED':
                    raise Exception(f"Critical Error: Report status returned '{check_status.status_code}' ...")

                else:
                    self.helper.exponential_backoff(n_status_checks)
                    n_status_checks += 1
            else:
                logging.error(f"Error: Failure to retrieve report status: {check_status.status_code}\n")
                self.helper.exponential_backoff(n_status_checks)
                n_status_checks += 1

        if document_id is None:
            raise Exception(f"\nCould not download the report after {n_status_checks} attempts.")

        document_url = f"{config.endpoint}reports/2021-06-30/documents/{document_id}"
        report_dl_request = req.get(document_url, headers={'x-amz-access-token': self.access_token})
        download_link = report_dl_request.json().get('url')
        request_download = req.get(download_link, stream=True)

        compression = report_dl_request.json().get('compressionAlgorithm', '')

        if compression == 'GZIP':
            report_contents = request_download.content

            with gzip.GzipFile(fileobj=io.BytesIO(report_contents)) as gz:
                report_contents = gz.read().decode('latin1')
        else:
            report_contents = request_download.text

        # ____________________________________________________________________________________________________________
        # CONVERT TO DATAFRAME AND RETURN
        # ____________________________________________________________________________________________________________

        try:
            orders_df = pd.read_csv(io.StringIO(report_contents), sep='\t', encoding='latin1')
            print('Successfully retrieved order report API data and converted to DataFrame.')
            print("_"*200)

            if len(orders_df) == 0:
                self.zero_sales = True
                logging.info(f"No sales for {self.start_date_str} - {self.end_date_str}")

            return orders_df

        except Exception as e:
            logging.error(f"Critical error occurred while writing downloaded report to DataFrame. \n{e}\n")
            raise e

    def compile_report(self, write_to_disk: bool = False, account_name: str = None) -> None:
        """
        Calls `get_orders_data` and `get_inventory_data` methods: wrangles & compiles the data to an .xlsx file.

        :param write_to_disk: (bool) If True, writes the df to the active project file directory. Else, holds in memory.
        :param account_name: (str): If specified, will print the account name to the report name and subject body.
        """
        # ____________________________________________________________________________________________________________
        # CALL GET ORDERS METHOD
        # ____________________________________________________________________________________________________________

        try:
            orders = self.get_orders_data()
        except Exception as e:
            logging.error(f"Unexpected error occurred whilst fetching orders data...{e}")
            raise e

        # ____________________________________________________________________________________________________________
        # CALL GET INVENTORY METHOD
        # ____________________________________________________________________________________________________________

        try:
            in_stock = self.get_inventory_data()

            if len(in_stock) == 0:
                self.zero_inventory = True
                logging.info(f"No inventory for this report.")
        except:
            # logging.error(f"{e} Proceeding with report, without 'Remaining Units' column")
            print("\tCould not generate inventory report - proceeding without 'Remaining Units' column ...\n")
            in_stock = pd.DataFrame(columns=['sku', 'afn-fulfillable-quantity'])
            self.zero_inventory = True
            logging.info('Error downloading inventory report - passing empty DataFrame instead.')

        # ____________________________________________________________________________________________________________
        # PROCESS THE `ORDERS` DF: CLEAN, AGGREGATE & JOIN
        # ____________________________________________________________________________________________________________

        orders['item-price'] = orders['item-price'].fillna(0)
        orders = orders.loc[(orders['item-price'] > 0)]

        orders['item-price'] = pd.to_numeric(orders['item-price'], errors='coerce')
        orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')

        orders_pivot = pd.pivot_table(
            orders,
            index=['sku', 'product-name'],
            values=['quantity', 'item-price'],
            aggfunc='sum'
        ).reset_index()

        # ____________________________________________________________________________________________________________
        # PROCESS THE `IN STOCK` DF (IF IT EXISTS): CLEAN, AGGREGATE & JOIN
        # ____________________________________________________________________________________________________________

        if not self.zero_sales:
            if not self.zero_inventory:
                final_df = \
                    pd.merge(
                        orders_pivot,
                        in_stock[['sku', 'afn-fulfillable-quantity']],
                        on='sku',
                        how='left'
                    )\
                    .rename({
                        'item-price': 'Revenue',
                        'afn-fulfillable-quantity': 'Remaining Units',
                        'quantity': 'Units Ordered'
                    },
                        errors='ignore',
                        axis=1
                    )

                select_columns = ['sku', 'product-name', 'Units Ordered', 'Revenue', 'Remaining Units']
                final_df = final_df[select_columns]

                self.total_revenue = pd.to_numeric(final_df['Revenue'], errors='coerce').sum()
                self.total_units = int(pd.to_numeric(final_df['Units Ordered'], errors='coerce').sum())

                grand_total_row = pd.DataFrame({
                    'sku': ['Grand Total'], 'product-name': [''],
                    'Units Ordered': [self.total_units], 'Revenue': [self.total_revenue], 'Remaining Units': ['']
                })

                final_df = pd \
                    .concat([final_df, grand_total_row]) \
                    .sort_values(['Units Ordered', 'product-name'], ascending=[0, 1]) \
                    .reset_index(drop=True)

            # if the inventory report fails, or has 0 inventory, just prepare report without 'Remaining Units' column
            else:
                final_df = orders_pivot.rename(columns={
                    'item-price': 'Revenue', 'quantity': 'Units Ordered'}, errors='ignore')

                select_columns = ['sku', 'product-name', 'Units Ordered', 'Revenue']
                final_df = final_df[select_columns]

                self.total_revenue = pd.to_numeric(final_df['Revenue'], errors='coerce').sum()
                self.total_units = int(pd.to_numeric(final_df['Units Ordered'], errors='coerce').sum())

                grand_total_row = pd.DataFrame({
                    'sku': ['Grand Total'], 'product-name': [''],
                    'Units Ordered': [self.total_units], 'Revenue': [self.total_revenue]
                })

                final_df = pd \
                    .concat([final_df, grand_total_row]) \
                    .sort_values(['Units Ordered', 'product-name'], ascending=[0, 1]) \
                    .reset_index(drop=True) \
                    .loc[lambda x: x['Units Ordered'] > 0]

        # if there are no sales, just return an empty DataFrame
        else:
            final_df = pd.DataFrame({
                'sku': ['No sales for specified date range'],
                'product-name': [''],
                'Units Ordered': [''],
                'Revenue': ['']
            })
        # ____________________________________________________________________________________________________________
        # DETERMINE REPORT NAME (WILL BE USED TO TITLE THE .XLSX REPORT AS WELL AS THE EMAIL SUBJECT)
        # ____________________________________________________________________________________________________________

        if account_name:
            account_name = str(re.sub(r'[/:*?"<>|\\]', '', account_name))  # strip to prevent file corruption

        if self.report_length_in_days <= 1:
            if not account_name:
                self.report_name = f"Daily Sales {self.start_date_str} ({self.total_units} units " \
                                   f"${self.total_revenue:,.2f})"
            else:
                self.report_name = \
                    f"{account_name.title()} Sales {self.start_date_str} ({self.total_units} units " \
                    f"${self.total_revenue:,.2f})"

        elif self.report_length_in_days > 1:
            if not account_name:
                self.report_name = \
                    f"Sales {self.start_date_str} through {self.end_date_str} ({self.total_units} units " \
                    f"${self.total_revenue:,.2f})"
            else:
                self.report_name = f"{account_name.title()} Sales {self.start_date_str} through {self.end_date_str} " \
                                   f"({self.total_units} units ${self.total_revenue:,.2f})"

        # ____________________________________________________________________________________________________________
        # OUTPUT THE WORKBOOK: WRITE EITHER TO DISK OR MEM DEPENDING ON THE PARAMETER PASSED
        # ____________________________________________________________________________________________________________

        self.write_to_disk = write_to_disk

        if self.write_to_disk:
            self.output_path = os.path.join(os.path.dirname(__file__), f"{self.report_name}.xlsx")
        else:
            self.output_path = io.BytesIO()
            self.output_path.seek(0)

        with pd.ExcelWriter(self.output_path) as writer:
            final_df.to_excel(writer, index=False, sheet_name=f"Summary Table")
            orders.to_excel(writer, index=False, sheet_name='Raw Sales Data')

        if not self.write_to_disk:
            self.output_path.seek(0)

        self._style_sales_report()
        print("An .xlsx report has been successfully generated for the orders/inventory data.\n")

    def _style_sales_report(self):
        """Helper method - formats the .xlsx file generated in the `compile_report` method with sexy styling."""

        wb = xl.load_workbook(self.output_path)
        ws = wb.active
        styler = Style(ws)

        styler.align_and_center()
        styler.create_table()

        # bold/highlight certain cells
        cells_to_fmt = {'A2': [True, False], 'C2': [True, True], 'D2': [True, True]}
        for cell, fmt in cells_to_fmt.items():
            styler.apply_styles_to_cell(cell=cell, bold=fmt[0], highlighter=fmt[1])

        # add currency and/or thousands separator to certain columns
        numeric_cols_fmt = {'C': False, 'E': False, 'D': True}
        for col, fmt in numeric_cols_fmt.items():
            styler.currency_formatter(col, currency=fmt)

        if self.write_to_disk is False:
            self.output_path.seek(0)  # file corrupts without this

        wb.save(self.output_path)

    def email_compiled_report(
            self, seed: str, from_email: str, to_email: Optional[Union[str, List[str]]], subject_text: str,
            body_text: str) -> None:
        """
        Sends email with the .xlsx attachment that is generated via the `compile_report` method.

        :param seed: (str) The security key for the senders email address
        :param from_email: (str) Senders email address
        :param to_email: (str) Recipient email address
        :param subject_text: (str) Subject text of the email
        :param body_text: (str) Body text of the email
        """
        # ____________________________________________________________________________________________________________
        # VALIDATION: IF EITHER OF THESE INSTANCE VARIABLES ARE NONE, THEN THE REPORT HAS NOT YET BEEN GENERATED
        # ____________________________________________________________________________________________________________

        if any([not self.report_name, not self.output_path]):
            raise ValueError("The Excel report has not yet been generated. Please run 'compile_reports' method first.")

        # ____________________________________________________________________________________________________________
        # DRAFTING THE EMAIL
        # ____________________________________________________________________________________________________________

        current_attempt = 1
        max_attempts = 5
        while current_attempt < max_attempts:

            try:
                em = EmailMessage()
                em['From'] = from_email
                em['To'] = to_email
                em['Subject'] = subject_text
                em.set_content(body_text)

                if isinstance(self.output_path, io.BytesIO):
                    file_data = self.output_path.getvalue()
                else:
                    with open(self.output_path, 'rb') as file:
                        file_data = file.read()

                em.add_attachment(
                    file_data,
                    maintype='application',
                    subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    filename=f"{self.report_name}.xlsx"
                )

                # ______________________________________________________________________________________________________
                # SENDING THE EMAIL
                # ______________________________________________________________________________________________________

                context = ssl.create_default_context()

                with SMTP_SSL('smtp.gmail.com', port=465, context=context) as smtp:
                    smtp.login(from_email, seed)
                    smtp.sendmail(from_email, to_email, em.as_string())

                print(f"Email successfully sent.\n")
                break

            except Exception as e:
                logging.error(f"Failure sending the email {e}")
                self.helper.exponential_backoff(current_attempt, base_seconds=5, rate_of_growth=1.5)
                current_attempt += 1

        else:
            raise Exception(f"Failed to send the email after {current_attempt} attempts.")


if __name__ == '__main__':
    start_time = datetime.today()

    daily_sales = SalesReport()
    daily_sales.compile_report(account_name=config.account_name, write_to_disk=True)

    if daily_sales.zero_sales:
        config.body_text = config.alt_body_text
        
    if daily_sales.zero_inventory:
        config.to_email = config.alt_to_email

    daily_sales.email_compiled_report(
        seed=config.gmail_seed,
        from_email=config.from_email,
        to_email=config.to_email,
        subject_text=daily_sales.report_name,
        body_text=config.body_text
    )

    print(f"Total program runtime: {datetime.today() - start_time}")
