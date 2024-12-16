import os
from dotenv import load_dotenv

load_dotenv()

NEW_CRM_API_KEY = os.getenv('NEW_CRM_API_KEY')
OLD_CRM_API_KEY = os.getenv('OLD_CRM_API_KEY')