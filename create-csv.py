import pandas as pd
import pymongo
import os
from dotenv import load_dotenv

load_dotenv()
client = pymongo.MongoClient(os.getenv("URL_MONGO"))
db = client["responses-llm"]

def consolidar_escala(nome_base, sufixos_modelos):
    print(f"Consolidando dados da escala: {nome_base}...")
    master_data = []
    col_original = db[f"{nome_base}_responses"]
    docs_base = col_original.find({}, {"_id": 1, "respostas": 1})
    
    for doc in docs_base:
        row = {"id": str(doc["_id"]), "respostas": doc.get("respostas")}
        for modelo in ["gpt", "deepseek", "llama"]:
            col_eval_name = f"eval_{nome_base}_{modelo}"
                
            eval_doc = db[col_eval_name].find_one({"original_id": doc["_id"]})
            
            if eval_doc:
                out = eval_doc.get("output_llm", {})
                row[f"{modelo}_status"] = out.get("status")
                row[f"{modelo}_score"] = out.get("score_total")
                row[f"{modelo}_alerta"] = out.get("alerta_de_risco")
            else:
                row[f"{modelo}_status"] = None
        
        master_data.append(row)
    
    df = pd.DataFrame(master_data)
    df.to_csv(f"consolidado_{nome_base}.csv", index=False)
    return df

df_phq = consolidar_escala("phq", ["gpt", "deepseek", "llama"])
df_beck = consolidar_escala("beck", ["gpt", "deepseek", "llama"])
df_gad7 = consolidar_escala("gad7", ["gpt", "deepseek", "llama"])

print("\nArquivos CSV gerados com sucesso!")