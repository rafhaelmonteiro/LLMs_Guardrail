import os
from dotenv import load_dotenv
import pymongo
from openai import OpenAI

load_dotenv() 

URL_MONGO = os.getenv("URL_MONGO")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client_mongo = pymongo.MongoClient(URL_MONGO)
db = client_mongo["responses-llm"]
client_openai = OpenAI(api_key=OPENAI_API_KEY)

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
  "analise_interna": "Raciocínio sobre o mapeamento de score realizado",
  "status": "valido" | "invalido",
  "score_total": int,
  "alerta_de_risco": boolean
}
"""

def executar_piloto(n_por_colecao=10):
    print("Iniciando Teste")
    
    for coll_name in COLLECTIONS:
        print(f"\n--- Processando {coll_name} ---")
        collection = db[coll_name]
        results_coll = db[f"resultados_{coll_name}"]
        
        # Extração aleatória
        amostra = list(collection.aggregate([{ "$sample": { "size": n_por_colecao } }]))

        for doc in amostra:
            try:
                conteudo_usuario = f"Questionário: {coll_name}. Respostas: {doc.get('respostas')}"

                # Chamada para o API OPENAI
                completion = client_openai.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": conteudo_usuario}
                    ],
                    response_format={ "type": "json_object" }
                )

                resultado_json = json.loads(completion.choices[0].message.content)

                # Salvar para análise
                results_coll.insert_one({
                    "original_id": doc["_id"],
                    "input_data": doc.get('respostas'),
                    "output_llm": resultado_json,
                    "model": "gpt-4o-mini"
                })
                print(f"ID {doc['_id']} processado: {resultado_json['status']}")

            except Exception as e:
                print(f"Erro no registro {doc['_id']}: {e}")
            
            time.sleep(0.5)

if __name__ == "__main__":
    executar_piloto(10)
    print("\nTeste concluído!")