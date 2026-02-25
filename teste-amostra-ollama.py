import os
import json
import time
import pymongo
import requests
from dotenv import load_dotenv

load_dotenv()

client_mongo = pymongo.MongoClient(os.getenv("URL_MONGO"))
db = client_mongo["responses-llm"]

MODELO = "llama3:latest" 
BASE_URL = "http://localhost:11434/api/chat" 
COLLECTIONS = ["phq_responses", "beck_responses", "gad7_responses"]

SYSTEM_PROMPT = """
Você é um Auditor Clínico de Dados. Sua missão é validar se as respostas de questionários de saúde mental estão íntegras.
### REGRAS DE OURO:
1. MAPEAMENTO LIKERT (PHQ-9 e GAD-7):
   - "Nenhuma vez" ou "Não" = 0
   - "Vários dias" = 1
   - "Mais da metade dos dias" = 2
   - "Quase todos os dias" = 3
   * Seja flexível com espaços ou letras maiúsculas.

2. INVENTÁRIO DE BECK (BDI):
   - O BDI tem 21 questões. Cada questão tem 4 opções.
   - Sua tarefa é inferir o valor (0, 1, 2 ou 3) com base na intensidade da frase.
   - Exemplo: "Não me sinto triste" (0) vs "Estou tão triste que não aguento" (3).
   - Se houver uma resposta textual para cada uma das 21 questões, considere VÁLIDO e atribua o score por intensidade.

3. CRITÉRIOS DE INVALIDADE:
   - Respostas que não tratam do assunto (ex: "Não sei", "Batata").
   - Questionários com perguntas faltando (PHQ < 9, GAD < 7, BDI < 21).

### SAÍDA JSON OBRIGATÓRIA:
{
  "analise_interna": "string",
  "status": "valido" | "invalido",
  "score_total": int,
  "alerta_de_risco": boolean
}
"""


def call_llama_local(system_content, user_content):
    payload = {
        "model": MODELO,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0}
    }
    
    response = requests.post(BASE_URL, json=payload, timeout=120)
    response.raise_for_status()
    return json.loads(response.json()["message"]["content"])

def executar_piloto_llama(n_por_colecao=10):
    print(f"Iniciando Teste Local com {MODELO}")
    
    for coll_name in COLLECTIONS:
        print(f"\n--- Processando {coll_name} ---")
        collection = db[coll_name]
        results_coll = db[f"amostragem_llama_{coll_name}"]
        
        amostra = list(collection.aggregate([{ "$sample": { "size": n_por_colecao } }]))

        for doc in amostra:
            try:
                conteudo_usuario = f"Questionário: {coll_name}. Respostas: {doc.get('respostas')}"
                resultado_json = call_llama_local(SYSTEM_PROMPT, conteudo_usuario)
                
                results_coll.insert_one({
                    "original_id": doc["_id"],
                    "output_llm": resultado_json,
                    "model": MODELO,
                    "timestamp": time.time()
                })
                print(f"ID {doc['_id']} OK: {resultado_json.get('status')}")
            except Exception as e:
                print(f"Erro no ID {doc['_id']}: {e}")

if __name__ == "__main__":
    executar_piloto_llama(10)