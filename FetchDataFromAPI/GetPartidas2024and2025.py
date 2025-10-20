import requests
import json
import os
from dotenv import load_dotenv


comeco = 2024

while comeco <= 2025:
    url = f"https://api.football-data.org/v4/competitions/BSA/matches?season={comeco}&dateFrom={comeco}-01-01&dateTo={comeco}-12-31"


    path = "/usr/src/Raw/Partidas/"
    os.makedirs(path, exist_ok=True)

    token = os.getenv("API_KEY")
    headers = {"X-Auth-Token": token}
    response = requests.get(url,headers=headers)

    if response.status_code == 200:
        data = response.json()
        print(data)
        with open(f"{path}partidas{comeco}.json","w") as f:
            json.dump(data,f,indent = 4)
    else:
        print("Erro:", response.status_code)
    comeco +=1
