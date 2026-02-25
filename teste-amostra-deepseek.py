import os
import json
import time
import pymongo
from dotenv import load_dotenv
from openai import OpenAI 

load_dotenv()

client_mongo = pymongo.MongoClient(os.getenv("URL_MONGO"))
db = client_mongo["responses-llm"]

client_deepseek = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"), 
    base_url="https://api.deepseek.com"
)

MODELO = "deepseek-chat" 
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

def executar_piloto_deepseek(n_por_colecao=10):
    print(f"Iniciando Teste de Amostragem com {MODELO}")
    
    for coll_name in COLLECTIONS:
        print(f"\n--- Processando {coll_name} ---")
        collection = db[coll_name]
        # Mudamos o nome da coleção de destino para diferenciar os resultados
        results_coll = db[f"amostragem_deepseek_{coll_name}"]
        
        amostra = list(collection.aggregate([{ "$sample": { "size": n_por_colecao } }]))

        for doc in amostra:
            sucesso = False
            tentativas = 0
            
            while not sucesso and tentativas < 5:
                try:
                    conteudo_usuario = f"Questionário: {coll_name}. Respostas: {doc.get('respostas')}"

                    response = client_deepseek.chat.completions.create(
                        model=MODELO,
                        messages=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": conteudo_usuario},
                        ],
                        response_format={'type': 'json_object'}, # Garante saída JSON
                        temperature=0.0
                    )
                    
                    resultado_json = json.loads(response.choices[0].message.content)
                    
                    results_coll.insert_one({
                        "original_id": doc["_id"],
                        "input_data": doc.get('respostas'),
                        "output_llm": resultado_json,
                        "model": MODELO,
                        "timestamp": time.time()
                    })
                    
                    print(f"ID {doc['_id']} OK: {resultado_json['status']}")
                    sucesso = True
                    time.sleep(1.0) # DeepSeek é rápido, 1s é suficiente para amostra

                except Exception as e:
                    if "429" in str(e):
                        print(f"Limite atingido. Aguardando 30s... (Tentativa {tentativas+1}/5)")
                        time.sleep(30)
                        tentativas += 1
                    else:
                        print(f"Erro inesperado no ID {doc['_id']}: {e}")
                        break 

if __name__ == "__main__":
    executar_piloto_deepseek(10)
    print("\nProcesso DeepSeek finalizado com sucesso!")