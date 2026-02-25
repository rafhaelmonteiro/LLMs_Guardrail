import os
import json
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from openai import AsyncOpenAI 
from motor.motor_asyncio import AsyncIOMotorClient
from tqdm.asyncio import tqdm

load_dotenv()

MONGO_URL = os.getenv("URL_MONGO")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") 
client_mongo = AsyncIOMotorClient(MONGO_URL)
db = client_mongo["responses-llm"]

client_openai = AsyncOpenAI(api_key=OPENAI_API_KEY)

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
   - Se houver uma resposta textual para cada uma das 21 questões, considere VÁLIDO e atribua o score.

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

MAP_COLLS = {
    "responses": "eval_final_phq",
    "beck_responses": "eval_final_beck",
    "gad7_responses": "eval_final_gad7"
}

async def executar_processamento_final():
    for source_coll, target_coll in MAP_COLLS.items():
        print(f"\n--- Processando: {source_coll} -> {target_coll} ---")
        
        collection = db[source_coll]
        results_coll = db[target_coll]

        # Busca todos os documentos
        cursor = collection.find()
        total_docs = await collection.count_documents({})
        
        async for doc in tqdm(cursor, total=total_docs, desc=f"Lote {source_coll}"):
            # Verifica se já foi processado para evitar gastos duplicados
            exists = await results_coll.find_one({"original_id": doc["_id"]})
            if exists:
                continue

            try:
                user_content = f"Questionário: {source_coll}. Dados: {doc.get('respostas')}"
                
                # Chamada assíncrona da OpenAI
                completion = await client_openai.chat.completions.create(
                    model="gpt-4o-mini", 
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_content}
                    ],
                    response_format={ "type": "json_object" }
                )

                res_text = completion.choices[0].message.content
                res_json = json.loads(res_text)
                
                # Gravação assíncrona no MongoDB
                await results_coll.insert_one({
                    "original_id": doc["_id"],
                    "participante_id": doc.get("participante_id"),
                    "output_llm": res_json,
                    "timestamp": datetime.now().isoformat()
                })

            except Exception as e:
                print(f"\n[ERRO] ID {doc['_id']}: {e}")
                await asyncio.sleep(1) 
if __name__ == "__main__":
    try:
        asyncio.run(executar_processamento_final())
        print("\n Processamento finalizado com sucesso!")
    except KeyboardInterrupt:
        print("\n Processo interrompido pelo usuário.")