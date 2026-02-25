import os
import time
import random
import requests
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from typing import List, Dict, Union
from pymongo.errors import ConnectionFailure


load_dotenv() 

URL_MONGO = os.getenv("URL_MONGO")

COLLECTIONS = ["phq_responses", "beck_responses", "gad7_responses"]

random.seed(42)

PHQ9_QUESTIONS = [
    "Pouco interesse ou pouco prazer em fazer as coisas",
    "Se sentir para baixo, deprimido(a) ou sem perspectiva",
    "Dificuldade para pegar no sono ou permanecer dormindo, ou dormir mais do que de costume",
    "Se sentir cansado(a) ou com pouca energia",
    "Falta de apetite ou comendo demais",
    "Se sentir mal consigo mesmo(a) — ou achar que você é um fracasso ou que decepcionou sua família ou você mesmo(a)",
    "Dificuldade para se concentrar nas coisas, como ler o jornal ou ver televisão",
    "Lentidão para se movimentar ou falar, a ponto das outras pessoas perceberem? Ou o oposto — estar tão agitado(a) ou irrequieto(a) que você fica andando de um lado para o outro muito mais do que de costume",
    "Pensar em se ferir de alguma forma ou que seria melhor estar morto(a)"
]

RESPONSE_OPTIONS = [
    "Nenhuma vez",
    "Vários dias", 
    "Mais da metade dos dias",
    "Quase todos os dias"
]

RESISTANT_RESPONSES = [
    "não quero responder", "não vou responder isso", "prefiro não dizer",
    "que pergunta idiota", "isso não te interessa", "o que te importa?",
    "me deixa em paz", "ah sim, super relevante isso", "que diferença faz?",
    "tanto faz", "quanto foi o jogo ontem?", "você viu o último episódio?",
    "que horas são?", "gosto de pizza", "azul elefante voador",
    "123 abc xyz", "???", "mais ou menos", "depende do dia",
    "normal", "estou cansado dessas perguntas", "que saco",
]


def connect_mongodb(mongo_uri: str, database_name: str = "responses-llm", collection_name: str = "responses"):
    """Conecta ao MongoDB e retorna a collection"""
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        
        db = client[database_name]
        collection = db[collection_name]
        
        print(f"✓ MongoDB conectado!")
        return client, collection
        
    except ConnectionFailure as e:
        print(f" Erro ao conectar MongoDB: {e}")
        raise


def generate_persona(participant_id: int, behavior_type: str) -> Dict:
    """Gera uma persona com tipo de comportamento"""
    return {
        "id": participant_id,
        "idade": random.randint(18, 75),
        "genero": random.choice(["masculino", "feminino"]),
        "behavior_type": behavior_type
    }


def create_prompt_adequate(persona: Dict, question: str) -> str:
    """Cria prompt para resposta adequada"""
    return f"""Atue como uma pessoa humana respondendo as perguntas do questionário PHQ-9.

PERFIL DA PESSOA:
- {persona['idade']} anos
- Gênero: {persona['genero']}

PERGUNTA (sobre as últimas 2 semanas):
"{question}"

OPÇÕES DE RESPOSTA:
0 = Nenhuma vez
1 = Vários dias
2 = Mais da metade dos dias
3 = Quase todos os dias

Responda APENAS com o número (0, 1, 2 ou 3).

RESPONDA APENAS O NÚMERO:"""


def create_prompt_resistant(persona: Dict, question: str) -> str:
    """Cria prompt para resposta inadequada/resistente"""
    return f"""Você está simulando uma pessoa RESISTENTE respondendo um questionário.

PERFIL: {persona['idade']} anos, {persona['genero']}, irritado e não cooperativo

PERGUNTA: "{question}"

Gere uma resposta INADEQUADA (NÃO use as opções válidas do questionário).

Tipos: recusa, agressividade, sarcasmo, fora de contexto, nonsense

RESPONDA APENAS COM A RESPOSTA INADEQUADA:"""


def call_ollama(prompt: str, model: str = "llama3", base_url: str = "http://localhost:11434") -> str:
    """Chama o Ollama e retorna a resposta"""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.0}
    }
    
    try:
        response = requests.post(f"{base_url}/api/generate", json=payload, timeout=60)
        response.raise_for_status()
        return response.json()["response"].strip()
    except Exception as e:
        raise Exception(f"Erro ao chamar Ollama: {e}")


def extract_score(llm_response: str) -> int:
    """Extrai o score numérico da resposta do LLM"""
    for char in llm_response:
        if char in ['0', '1', '2', '3']:
            return int(char)
    return None


def generate_responses_for_persona(persona: Dict, model: str = "llama3") -> List[Dict]:
    """Gera todas as 9 respostas baseado no tipo de comportamento"""
    responses = []
    behavior = persona["behavior_type"]
    
    # Define quantas respostas serão adequadas
    if behavior == "cooperativo":
        num_adequadas = 9  # 100%
    elif behavior == "parcialmente_resistente":
        num_adequadas = random.randint(3, 5)  # 30-50%
    else:  # resistente
        num_adequadas = 0 
    
    # Define quais perguntas terão respostas adequadas
    questions_indices = list(range(9))
    random.shuffle(questions_indices)
    adequate_indices = set(questions_indices[:num_adequadas])
    
    for i, question in enumerate(PHQ9_QUESTIONS):
        if i in adequate_indices:
            # Resposta adequada
            prompt = create_prompt_adequate(persona, question)
            llm_response = call_ollama(prompt, model)
            score = extract_score(llm_response)
            resposta = RESPONSE_OPTIONS[score]
        else:
            # Resposta inadequada (60% LLM, 40% lista)
            if random.random() < 0.6:
                prompt = create_prompt_resistant(persona, question)
                resposta = call_ollama(prompt, model)[:100]
            else:
                resposta = random.choice(RESISTANT_RESPONSES)
        
        responses.append({
            "numero_questao": i + 1,
            "questao": question,
            "resposta": resposta
        })
        
        time.sleep(0.1)
    
    return responses


def check_ollama_availability(base_url: str = "http://localhost:11434") -> bool:
    """Verifica se Ollama está disponível"""
    try:
        response = requests.get(f"{base_url}/api/tags", timeout=5)
        return response.status_code == 200
    except:
        return False


def run_pipeline(num_participants: int = 600,
                mongo_uri: str = "mongodb://localhost:27017/",
                database_name: str = "responses-llm",
                collection_name: str = "responses",
                model: str = "llama3",
                behavior_distribution: Union[None, str, Dict[str, int]] = None):
    """
    Executa o pipeline completo
    
    Args:
        behavior_distribution: Pode ser:
            - None: distribuição padrão (70% cooperativo, 20% parcial, 10% resistente)
            - String: 'cooperativo', 'parcialmente_resistente' ou 'resistente' (todos desse tipo)
            - Dict: {"cooperativo": 420, "parcialmente_resistente": 120, "resistente": 60}
    """
    
    print("\n" + "=" * 70)
    print(" " * 20 + "PIPELINE PHQ-9")
    print("=" * 70)
    
    # Verifica Ollama
    print("\n✓ Verificando Ollama...")
    if not check_ollama_availability():
        raise Exception(" Ollama não disponível. Execute: ollama serve")
    print("✓ Ollama conectado!")
    
    # Conecta MongoDB
    print("\n✓ Conectando ao MongoDB...")
    client, collection = connect_mongodb(mongo_uri, database_name, collection_name)
    
    existing_count = collection.count_documents({})
    print(f"  Documentos existentes: {existing_count}")
    
    # Processa distribuição do comportamento
    if behavior_distribution is None:
        # Padrão: 70-20-10
        behavior_distribution = {
            "cooperativo": int(num_participants * 0.70),
            "parcialmente_resistente": int(num_participants * 0.20),
            "resistente": int(num_participants * 0.10)
        }
        diff = num_participants - sum(behavior_distribution.values())
        behavior_distribution["cooperativo"] += diff
        
    elif isinstance(behavior_distribution, str):
        # Todos do mesmo tipo
        valid = ["cooperativo", "parcialmente_resistente", "resistente"]
        if behavior_distribution not in valid:
            raise ValueError(f"Comportamento inválido: '{behavior_distribution}'. Use: {valid}")
        behavior_distribution = {behavior_distribution: num_participants}
        
    elif isinstance(behavior_distribution, dict):
        # Validar soma
        total = sum(behavior_distribution.values())
        if total != num_participants:
            raise ValueError(f"Distribuição soma {total}, mas num_participants é {num_participants}")
    else:
        raise ValueError("behavior_distribution deve ser None, string ou dict")
    
    # Gera personas
    print(f"\n👥 Gerando {num_participants} personas...")
    personas = []
    current_id = existing_count + 1
    
    for behavior_type, count in behavior_distribution.items():
        for _ in range(count):
            personas.append(generate_persona(current_id, behavior_type))
            current_id += 1
    
    random.shuffle(personas)
    
    # Gera e salva respostas
    saved_count = 0
    error_count = 0
    
    print(f"\n Gerando respostas usando {model}...")
    
    with tqdm(total=len(personas), desc="Processando", unit="pessoa") as pbar:
        for persona in personas:
            try:
                responses = generate_responses_for_persona(persona, model)
                
                document = {
                    "participante_id": persona["id"],
                    "persona": {
                        "idade": persona["idade"],
                        "genero": persona["genero"]
                    },
                    "respostas": responses,
                    "timestamp_geracao": datetime.now().isoformat(),
                    "modelo_usado": model,
                    "questionario": "PHQ-9"
                }
                
                collection.insert_one(document)
                saved_count += 1
                pbar.update(1)
                
            except Exception as e:
                error_count += 1
                print(f"\n Erro: {e}")
                pbar.update(1)
                continue
    
    # Resultados
    print("\n" + "=" * 70)
    print(" PIPELINE CONCLUÍDO!")
    print("=" * 70)
    
    total_in_db = collection.count_documents({})
    
    print(f"\n Resultados:")
    print(f"  Processados: {num_participants}")
    print(f"  Salvos: {saved_count}")
    print(f"  Erros: {error_count}")
    print(f"  Total no MongoDB: {total_in_db}")
    
    client.close()
    print("\n✓ Conexão MongoDB fechada")


if __name__ == "__main__":
    try:
        
        NUM_PARTICIPANTS = 6
        MONGO_URI = URL_MONGO
        DATABASE_NAME = "responses-llm"
        COLLECTION_NAME = "responses"
        MODEL = "llama3"
        
        # Opção 1: Distribuição padrão (70-20-10)
        BEHAVIOR_DISTRIBUTION = None
        
        # Opção 2: Todos do mesmo tipo (descomente para usar)
        # BEHAVIOR_DISTRIBUTION = 'cooperativo'              # Todos 100% corretas
        # BEHAVIOR_DISTRIBUTION = 'parcialmente_resistente'  # Todos 30-50% corretas
        # BEHAVIOR_DISTRIBUTION = 'resistente'               # Todos 0% corretas
        
        run_pipeline(
            num_participants=NUM_PARTICIPANTS,
            mongo_uri=MONGO_URI,
            database_name=DATABASE_NAME,
            collection_name=COLLECTION_NAME,
            model=MODEL,
            behavior_distribution=BEHAVIOR_DISTRIBUTION
        )
        
        print("\n Dados salvos com sucesso!")
        
    except Exception as e:
        print(f"\n Erro: {e}")
        raise