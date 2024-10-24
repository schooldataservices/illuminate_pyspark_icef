import requests
import time
import logging
from .config import *


# Configuration
TOKEN_URL = f'{token_url_illuminate}?OAuth2_AccessToken'
TOKEN_EXPIRY = 3600  # Token expiry time in seconds (1 hour)

def get_access_token():
    # Prepare the payload for the token request
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials',  # Assuming client credentials grant type
    }
    
    # Request the access token
    try:
        response = requests.post(TOKEN_URL, data=payload)
        logging.info('Calling API token endpoint')
    except Exception as e:
        logging.error(f'Unable to get API token succesfully due to {e}')
    
    if response.status_code == 200:
        logging.info('Succesfully retrieved API token')
        token_info = response.json()
        access_token = token_info.get('access_token')
        expires_in = token_info.get('expires_in', TOKEN_EXPIRY)  # Default to 1 hour if not provided
        
        return access_token, expires_in
    else:
        logging.error(f'Failed to obtain access token: {response.status_code} {response.text}')
        print('Failed to obtain access token:', response.status_code, response.text)
        return None, None
        
