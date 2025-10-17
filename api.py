import requests
import json
import os
from dotenv import load_dotenv



url = "https://api.football-data.org/v4/competitions/BSA/teams"

path = f"/usr/src/app/Raw/Times/"
os.makedirs(path, exist_ok=True)

token = os.getenv("API_KEY")
headers = {"X-Auth-Token": token}
response = requests.get(url,headers=headers)

if response.status_code == 200:
    data = response.json()
    print(data)
    with open(f"/usr/src/app/Raw/Times/TimesBrasileiros.json","w") as f:
        json.dump(data,f,indent = 4)
else:
    print("Erro:", response.status_code)

git remote add origin https://github.com/pichee/DadosDefutebol.git