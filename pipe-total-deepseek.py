import os
import json
import asyncio
import motor.motor_asyncio
from dotenv import load_dotenv
from openai import AsyncOpenAI 
from tqdm.asyncio import tqdm

load_dotenv()

client_mongo = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("URL_MONGO"))
db = client_mongo["responses-llm"]

client_deepseek = AsyncOpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"), 
    base_url="https://api.deepseek.com"
)

MODELO = "deepseek-chat"
COLLECTIONS = ["phq_responses", "beck_responses", "gad7_responses"]
CONCORRENCIA_MAXIMA = 20 

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

semaphore = asyncio.Semaphore(CONCORRENCIA_MAXIMA)

async def processar_registro(doc, coll_name, pbar):
    async with semaphore:
        destino_coll = db[f"eval_final_{coll_name}_deepseek"]
        
        # Checkpoint: não processa o que já existe
        if await destino_coll.find_one({"original_id": doc["_id"]}):
            pbar.update(1)
            return

        try:
            conteudo_usuario = f"Questionário: {coll_name}. Respostas: {doc.get('respostas')}"
            
            response = await client_deepseek.chat.completions.create(
                model=MODELO,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": conteudo_usuario},
                ],
                response_format={'type': 'json_object'},
                temperature=0.0,
                timeout=30.0
            )

            resultado_json = json.loads(response.choices[0].message.content)
            
            await destino_coll.insert_one({
                "original_id": doc["_id"],
                "input_data": doc.get('respostas'),
                "output_llm": resultado_json,
                "model": MODELO,
                "timestamp": asyncio.get_event_loop().time()
            })
        except Exception as e:
            await db["logs_erro_deepseek"].insert_one({"id": doc["_id"], "erro": str(e)})
        finally:
            pbar.update(1)

async def main():
    for coll_name in COLLECTIONS:
        collection = db[coll_name]
        cursor = collection.find({})
        total_docs = await collection.count_documents({})
        
        print(f"\nIniciando Big Run: {coll_name} ({total_docs} docs)")
        
        with tqdm(total=total_docs, desc=f"Processando {coll_name}") as pbar:
            tarefas = [] 
            async for doc in cursor:
                tarefas.append(processar_registro(doc, coll_name, pbar))
            
            await asyncio.gather(*tarefas)

if __name__ == "__main__":
    asyncio.run(main())