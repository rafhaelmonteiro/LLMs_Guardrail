import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

client_mongo = pymongo.MongoClient(os.getenv("URL_MONGO"))
db = client_mongo["responses-llm"]

COLLECTIONS_PILOTO = ["amostragem_llama_beck_responses", "amostragem_llama_gad7_responses", "amostragem_llama_phq_responses"]

def diagnosticar_falhas():
    print("=== RELATÓRIO DE DIAGNÓSTICO DE FALHAS (GUARDRAIL) ===\n")
    
    for coll_name in COLLECTIONS_PILOTO:
        coll = db[coll_name]
        # Buscamos apenas os registros que o LLM marcou como inválidos
        invalidos = list(coll.find({"output_llm.status": "invalido"}))
        
        print(f"Coleção: {coll_name}")
        print(f"Total de 'inválidos' encontrados: {len(invalidos)}")
        print("-" * 50)
        
        for doc in invalidos[:5]:  
            analise = doc['output_llm'].get('analise_interna', 'Sem análise interna')
            identificador = doc['original_id']
            
            print(f"ID Original: {identificador}")
            print(f"Motivo do LLM: {analise}")
            print("." * 30)
        print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    diagnosticar_falhas()