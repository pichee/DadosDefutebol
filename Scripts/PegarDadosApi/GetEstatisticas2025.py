import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv


today = datetime.today()
year = today.year
month = today.month
day = today.day

url = "https://api.football-data.org/v4/competitions/BSA/scorers?season=2025&limit=1000"

path = f"/usr/src/app/Raw/Estatisticas/year={year}/month={month}/day={day}"
os.makedirs(path, exist_ok=True)

token = os.getenv("API_KEY")
headers = {"X-Auth-Token": token}
response = requests.get(url,headers=headers)

if response.status_code == 200:
    data = response.json()
    with open(f"{path}/estatisticas{2025}.json","w") as f:
        json.dump(data,f,indent = 4)
else:
    print("Erro:", response.status_code)
