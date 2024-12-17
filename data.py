import os
from dotenv import load_dotenv

from espo_api_client import EspoAPI

load_dotenv()

NEW_CRM_API_KEY = os.getenv('NEW_CRM_API_KEY')
OLD_CRM_API_KEY = os.getenv('OLD_CRM_API_KEY')

NEW_URL = "https://www.crm.alis-is.com"
OLD_URL = "https://www.alis-is.com"

NEW_CLIENT = EspoAPI(NEW_URL, NEW_CRM_API_KEY)
OLD_CLIENT = EspoAPI(OLD_URL, OLD_CRM_API_KEY)