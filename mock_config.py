# _____________________________________________________________________________________________________________________
#                                               FOR INTERNAL USE ONLY
#                                               [REDACTED] TRADING CO.
# _____________________________________________________________________________________________________________________

# TODO rename this file to 'config.py' if you plan on running it. 

# Amazon SP-API dev keys:
account_name = 'Enter account name for your Amazon account here (optional)'
credentials = dict(
    client_identifier='',
    client_secret='',
    refresh_token='',
    rotation_deadline=''
)

# API parameters
endpoint = 'https://sellingpartnerapi-na.amazon.com/'
token_request_url = "https://api.amazon.com/auth/o2/token"
marketplace_id = 'ATVPDKIKX0DER'  # North America

# Email parameters
gmail_seed = 'Enter your email seed phrase here'
from_email = 'Enter your email address here (corresponding to your seed)'
to_email = 'Enter the recipient address here'
body_text = 'Good morning, Here is the sales report...'
alt_body_text = 'Good morning, there are no sales for this date range...'
