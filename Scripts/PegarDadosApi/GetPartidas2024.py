import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
today = datetime.today()
year = today.year
month = today.month
day = today.day
comeco = 2024
url = f"https://api.football-data.org/v4/competitions/BSA/matches?season={comeco}&dateFrom={comeco}-01-01&dateTo={comeco}-12-31"


path = f"/usr/src/app/Raw/Partidas/year=2024/month={month}/day={day}"
os.makedirs(path, exist_ok=True)

token = os.getenv("API_KEY")
headers = {"X-Auth-Token": token}
response = requests.get(url,headers=headers)

if response.status_code == 200:
    data = response.json()
    with open(f"{path}/partidas{comeco}.json","w") as f:
        json.dump(data,f,indent = 4)
else:
    print("Erro:", response.status_code)
