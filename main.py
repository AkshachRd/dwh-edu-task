from dotenv import load_dotenv
import requests
from os import environ

load_dotenv()
CURRENCY_API_KEY = environ.get("CURRENCY_API_KEY")

url = "https://currency-converter5.p.rapidapi.com/currency/convert"
querystring = {"format": "json", "from": "USD", "to": "RUB, EUR, CNY", "amount": "1"}
headers = {
    'x-rapidapi-key': CURRENCY_API_KEY,
    'x-rapidapi-host': "currency-converter5.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)