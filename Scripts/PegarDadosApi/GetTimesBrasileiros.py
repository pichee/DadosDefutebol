import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

today = datetime.today()
year = today.year
month = today.month
day = today.day

url = "https://api.football-data.org/v4/competitions/BSA/teams"

path = f"/usr/src/app/Raw/Times/year={year}/month={month}/day={day}"
os.makedirs(path, exist_ok=True)

token = os.getenv("API_KEY")
headers = {"X-Auth-Token": token}
response = requests.get(url,headers=headers)

if response.status_code == 200:
    data = response.json()
    with open(f"/usr/src/app/Raw/Times/year={year}/month={month}/day={day}/TimesBrasileiros.json","w") as f:
        json.dump(data,f,indent = 4)
else:
    print("Erro:", response.status_code)
