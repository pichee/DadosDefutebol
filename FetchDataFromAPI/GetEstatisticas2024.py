import requests
import json
import os
from dotenv import load_dotenv

url = "https://api.football-data.org/v4/competitions/BSA/scorers?season=2024&limit=1000"

path = f"/usr/src/app/Raw/Estatisticas/"
os.makedirs(path, exist_ok=True)

token = os.getenv("API_KEY")
headers = {"X-Auth-Token": token}
response = requests.get(url,headers=headers)

if response.status_code == 200:
    data = response.json()
    print(data)
    with open(f"/usr/src/app/Raw/Estatisticas/estatisticas2024.json","w") as f:
        json.dump(data,f,indent = 4)
else:
    print("Erro:", response.status_code)
