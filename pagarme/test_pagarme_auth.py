import os
import sys
import json
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=dotenv_path)
BASE_URL = "https://api.pagar.me/core/v5"

def main():
    secret_key = os.getenv("PAGARME_SECRET_KEY")
    # Diagnóstico do .env e variável (não expõe a chave)
    try:
        print(f".env path: {dotenv_path}")
        print(f".env exists: {dotenv_path.exists()}")
    except Exception:
        pass
    print("PAGARME_SECRET_KEY loaded:", bool(secret_key))
    if secret_key:
        print("PAGARME_SECRET_KEY prefix:", secret_key[:3])  # esperado: 'sk_'
        print("PAGARME_SECRET_KEY length:", len(secret_key))
    if not secret_key:
        print("ERRO: defina a variável de ambiente PAGARME_SECRET_KEY com sua secret key (ex.: sk_test_...)", file=sys.stderr)
        sys.exit(1)

    url = f"{BASE_URL}/customers"
    try:
        resp = requests.get(url, auth=HTTPBasicAuth(secret_key, ""), headers={"Accept": "application/json"}, timeout=20)
    except requests.RequestException as e:
        print(f"ERRO DE REDE: {e}", file=sys.stderr)
        sys.exit(2)

    print("Status:", resp.status_code)
    try:
        data = resp.json()
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except ValueError:
        print("Resposta não-JSON:")
        print(resp.text)

    if resp.status_code == 401:
        print("\nDiagnóstico: 401 indica chave inválida/ambiente incorreto ou Basic Auth malformado.")
    elif resp.status_code >= 400:
        print("\nDiagnóstico: houve erro. Verifique chave, permissões e endpoint.")

if __name__ == "__main__":
    main()
