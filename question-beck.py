import random
import os
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime
from typing import List, Dict, Union
import requests
from tqdm import tqdm
import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

random.seed(42)

load_dotenv() 
URL_MONGO = os.getenv("URL_MONGO")


BECK_QUESTIONS = [
    # Questão 1 - Tristeza
    {
        "numero": 1,
        "categoria": "Tristeza",
        "opcoes": [
            "Não me sinto triste",
            "Eu me sinto triste",
            "Estou sempre triste e não consigo sair disto",
            "Estou tão triste ou infeliz que não consigo suportar"
        ]
    },
    # Questão 2 - Pessimismo
    {
        "numero": 2,
        "categoria": "Pessimismo",
        "opcoes": [
            "Não estou especialmente desanimado quanto ao futuro",
            "Eu me sinto desanimado quanto ao futuro",
            "Acho que nada tenho a esperar",
            "Acho o futuro sem esperança e tenho a impressão de que as coisas não podem melhorar"
        ]
    },
    # Questão 3 - Fracasso
    {
        "numero": 3,
        "categoria": "Fracasso",
        "opcoes": [
            "Não me sinto um fracasso",
            "Acho que fracassei mais do que uma pessoa comum",
            "Quando olho para trás, na minha vida, tudo o que posso ver é um monte de fracassos",
            "Acho que, como pessoa, sou um completo fracasso"
        ]
    },
    # Questão 4 - Perda de Prazer
    {
        "numero": 4,
        "categoria": "Perda de Prazer",
        "opcoes": [
            "Tenho tanto prazer em tudo como antes",
            "Não sinto mais prazer nas coisas como antes",
            "Não encontro um prazer real em mais nada",
            "Estou insatisfeito ou aborrecido com tudo"
        ]
    },
    # Questão 5 - Sentimentos de Culpa
    {
        "numero": 5,
        "categoria": "Sentimentos de Culpa",
        "opcoes": [
            "Não me sinto especialmente culpado",
            "Eu me sinto culpado às vezes",
            "Eu me sinto culpado na maior parte do tempo",
            "Eu me sinto sempre culpado"
        ]
    },
    # Questão 6 - Sentimentos de Punição
    {
        "numero": 6,
        "categoria": "Sentimentos de Punição",
        "opcoes": [
            "Não acho que esteja sendo punido",
            "Acho que posso ser punido",
            "Creio que vou ser punido",
            "Acho que estou sendo punido"
        ]
    },
    # Questão 7 - Autoestima
    {
        "numero": 7,
        "categoria": "Autoestima",
        "opcoes": [
            "Não me sinto decepcionado comigo mesmo",
            "Estou decepcionado comigo mesmo",
            "Estou enojado de mim",
            "Eu me odeio"
        ]
    },
    # Questão 8 - Autocrítica
    {
        "numero": 8,
        "categoria": "Autocrítica",
        "opcoes": [
            "Não me sinto de qualquer modo pior que os outros",
            "Sou crítico em relação a mim por minhas fraquezas ou erros",
            "Eu me culpo sempre por minhas falhas",
            "Eu me culpo por tudo de mal que acontece"
        ]
    },
    # Questão 9 - Pensamentos Suicidas
    {
        "numero": 9,
        "categoria": "Pensamentos Suicidas",
        "opcoes": [
            "Não tenho quaisquer ideias de me matar",
            "Tenho ideias de me matar, mas não as executaria",
            "Gostaria de me matar",
            "Eu me mataria se tivesse oportunidade"
        ]
    },
    # Questão 10 - Choro
    {
        "numero": 10,
        "categoria": "Choro",
        "opcoes": [
            "Não choro mais que o habitual",
            "Choro mais agora do que costumava",
            "Agora, choro o tempo todo",
            "Costumava ser capaz de chorar, mas agora não consigo mesmo que o queira"
        ]
    },
    # Questão 11 - Agitação
    {
        "numero": 11,
        "categoria": "Agitação",
        "opcoes": [
            "Não sou mais irritado agora do que já fui",
            "Fico aborrecido ou irritado mais facilmente do que costumava",
            "Atualmente me sinto irritado o tempo todo",
            "Não me irrito mais com as coisas que costumavam me irritar"
        ]
    },
    # Questão 12 - Perda de Interesse
    {
        "numero": 12,
        "categoria": "Perda de Interesse",
        "opcoes": [
            "Não perdi o interesse nas outras pessoas",
            "Interesso-me menos do que costumava pelas outras pessoas",
            "Perdi a maior parte do meu interesse nas outras pessoas",
            "Perdi todo o meu interesse nas outras pessoas"
        ]
    },
    # Questão 13 - Indecisão
    {
        "numero": 13,
        "categoria": "Indecisão",
        "opcoes": [
            "Tomo decisões quase tão bem como em outrora",
            "Adio minhas decisões mais do que costumava",
            "Tenho maior dificuldade em tomar decisões do que antes",
            "Não consigo mais tomar decisões"
        ]
    },
    # Questão 14 - Mudança na Autoimagem
    {
        "numero": 14,
        "categoria": "Mudança na Autoimagem",
        "opcoes": [
            "Não sinto que minha aparência seja pior do que costumava ser",
            "Preocupo-me por estar parecendo velho ou sem atrativos",
            "Sinto que há mudanças permanentes em minha aparência que me fazem parecer sem atrativos",
            "Considero-me feio"
        ]
    },
    # Questão 15 - Dificuldade de Trabalhar
    {
        "numero": 15,
        "categoria": "Dificuldade de Trabalhar",
        "opcoes": [
            "Posso trabalhar tão bem quanto antes",
            "Preciso de um esforço extra para começar qualquer coisa",
            "Tenho de me esforçar muito até fazer qualquer coisa",
            "Não consigo fazer nenhum trabalho"
        ]
    },
    # Questão 16 - Perturbação do Sono
    {
        "numero": 16,
        "categoria": "Perturbação do Sono",
        "opcoes": [
            "Durmo tão bem quanto de costume",
            "Não durmo tão bem quanto costumava",
            "Acordo uma ou duas horas mais cedo do que de costume e tenho dificuldade para voltar a dormir",
            "Acordo várias horas mais cedo do que costumava e tenho dificuldade para voltar a dormir"
        ]
    },
    # Questão 17 - Fadiga
    {
        "numero": 17,
        "categoria": "Fadiga",
        "opcoes": [
            "Não fico mais cansado do que de costume",
            "Fico cansado com mais facilidade do que costumava",
            "Sinto-me cansado ao fazer quase qualquer coisa",
            "Estou cansado demais para fazer qualquer coisa"
        ]
    },
    # Questão 18 - Perda de Apetite
    {
        "numero": 18,
        "categoria": "Perda de Apetite",
        "opcoes": [
            "Meu apetite não está pior do que de costume",
            "Meu apetite não é tão bom quanto costumava ser",
            "Meu apetite está muito pior agora",
            "Não tenho mais nenhum apetite"
        ]
    },
    # Questão 19 - Perda de Peso
    {
        "numero": 19,
        "categoria": "Perda de Peso",
        "opcoes": [
            "Não perdi muito peso, se é que perdi algum ultimamente",
            "Perdi mais de 2,5 Kg",
            "Perdi mais de 5,0 Kg",
            "Perdi mais de 7,5 Kg"
        ]
    },
    # Questão 20 - Preocupação Somática
    {
        "numero": 20,
        "categoria": "Preocupação Somática",
        "opcoes": [
            "Não me preocupo mais que o usual com minha saúde",
            "Preocupo-me com problemas físicos como dores e aflições, mal-estar no estômago ou prisão de ventre",
            "Estou muito preocupado com problemas físicos e é difícil pensar em outra coisa",
            "Estou tão preocupado com meus problemas físicos que não consigo pensar em outra coisa"
        ]
    },
    # Questão 21 - Perda de Libido
    {
        "numero": 21,
        "categoria": "Perda de Libido",
        "opcoes": [
            "Não tenho observado qualquer mudança recente em meu interesse sexual",
            "Estou menos interessado por sexo do que costumava",
            "Estou bem menos interessado em sexo atualmente",
            "Perdi completamente o interesse por sexo"
        ]
    }
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


def connect_mongodb(mongo_uri: str, database_name: str = "responses-llm", collection_name: str = "beck_responses"):
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


def create_prompt_adequate(persona: Dict, question: Dict) -> str:
    """Cria prompt para resposta adequada ao Inventário de Beck"""
    opcoes_texto = "\n".join([f"{i} = {opcao}" for i, opcao in enumerate(question["opcoes"])])
    
    return f"""Atue como uma pessoa humana respondendo o Inventário de Depressão de Beck (BDI-II).

PERFIL DA PESSOA:
- {persona['idade']} anos
- Gênero: {persona['genero']}

PERGUNTA (como você se sentiu na última semana):
Categoria: {question['categoria']}

OPÇÕES DE RESPOSTA:
{opcoes_texto}

Responda APENAS com o número (0, 1, 2 ou 3) que melhor descreve como você se sentiu.

RESPONDA APENAS O NÚMERO:"""


def create_prompt_resistant(persona: Dict, question: Dict) -> str:
    """Cria prompt para resposta inadequada/resistente"""
    return f"""Você está simulando uma pessoa RESISTENTE respondendo um questionário psicológico.

PERFIL: {persona['idade']} anos, {persona['genero']}, irritado e não cooperativo

PERGUNTA sobre: {question['categoria']}

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
    """Extrai o score numérico da resposta do LLM (0-3 para Beck)"""
    for char in llm_response:
        if char in ['0', '1', '2', '3']:
            return int(char)
    return None


def generate_responses_for_persona(persona: Dict, model: str = "llama3") -> List[Dict]:
    """Gera todas as 21 respostas do Inventário de Beck"""
    responses = []
    behavior = persona["behavior_type"]
    
    # Define quantas respostas serão adequadas (agora são 21 questões)
    if behavior == "cooperativo":
        num_adequadas = 21  # 100%
    elif behavior == "parcialmente_resistente":
        num_adequadas = random.randint(7, 11)  # 30-50% de 21
    else:  # resistente
        num_adequadas = 0  # 0%
    
    # Define quais perguntas terão respostas adequadas
    questions_indices = list(range(21))
    random.shuffle(questions_indices)
    adequate_indices = set(questions_indices[:num_adequadas])
    
    for i, question in enumerate(BECK_QUESTIONS):
        if i in adequate_indices:
            # Resposta adequada
            prompt = create_prompt_adequate(persona, question)
            llm_response = call_ollama(prompt, model)
            score = extract_score(llm_response)
            resposta = question["opcoes"][score]
        else:
            # Resposta inadequada (60% LLM, 40% lista)
            if random.random() < 0.6:
                prompt = create_prompt_resistant(persona, question)
                resposta = call_ollama(prompt, model)[:100]
            else:
                resposta = random.choice(RESISTANT_RESPONSES)
        
        responses.append({
            "numero_questao": question["numero"],
            "categoria": question["categoria"],
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


def run_pipeline(num_participants: int = 300,
                mongo_uri: str = "mongodb://localhost:27017/",
                database_name: str = "responses-llm",
                collection_name: str = "beck_responses",
                model: str = "llama3",
                behavior_distribution: Union[None, str, Dict[str, int]] = None):
    """
    Executa o pipeline completo para Inventário de Beck
    
    Args:
        behavior_distribution: Pode ser:
            - None: distribuição padrão (70% cooperativo, 20% parcial, 10% resistente)
            - String: 'cooperativo', 'parcialmente_resistente' ou 'resistente'
            - Dict: {"cooperativo": 420, "parcialmente_resistente": 120, "resistente": 60}
    """
    
    print("\n" + "=" * 70)
    print(" " * 15 + "PIPELINE INVENTÁRIO DE BECK (BDI-II)")
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
        behavior_distribution = {
            "cooperativo": int(num_participants * 0.70),
            "parcialmente_resistente": int(num_participants * 0.20),
            "resistente": int(num_participants * 0.10)
        }
        diff = num_participants - sum(behavior_distribution.values())
        behavior_distribution["cooperativo"] += diff
        
    elif isinstance(behavior_distribution, str):
        valid = ["cooperativo", "parcialmente_resistente", "resistente"]
        if behavior_distribution not in valid:
            raise ValueError(f"Comportamento inválido: '{behavior_distribution}'. Use: {valid}")
        behavior_distribution = {behavior_distribution: num_participants}
        
    elif isinstance(behavior_distribution, dict):
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
    print(" 21 questões do Inventário de Beck por participante")
    
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
                    "questionario": "BDI-II (Inventário de Depressão de Beck)"
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
    
    print(f"\n RESULTADOS:")
    print(f"  Processados: {num_participants}")
    print(f"  Salvos: {saved_count}")
    print(f"  Erros: {error_count}")
    print(f"  Total no MongoDB: {total_in_db}")
    print(f"  Total de respostas: {saved_count * 21}")
    
    print(f"\nCOMPORTAMENTOS NO MONGODB:")
    for behavior in ["cooperativo", "parcialmente_resistente", "resistente"]:
        count = collection.count_documents({"behavior_type": behavior})
        if count > 0:
            print(f"  {behavior}: {count}")
    
    client.close()
    print("\n✓ Conexão MongoDB fechada")


if __name__ == "__main__":
    try:
        NUM_PARTICIPANTS = 125
        MONGO_URI = URL_MONGO
        DATABASE_NAME = "responses-llm"
        COLLECTION_NAME = "beck_responses"
        MODEL = "llama3"
        
        # Opção 1: Distribuição padrão (70-20-10)
        #BEHAVIOR_DISTRIBUTION = None
        
        # Opção 2: Todos do mesmo tipo (descomente para usar)
        # BEHAVIOR_DISTRIBUTION = 'cooperativo'              # Todos 100% corretas
        BEHAVIOR_DISTRIBUTION = 'parcialmente_resistente'  # Todos 30-50% corretas
        # BEHAVIOR_DISTRIBUTION = 'resistente'               # Todos 0% corretas
        
        run_pipeline(
            num_participants=NUM_PARTICIPANTS,
            mongo_uri=MONGO_URI,
            database_name=DATABASE_NAME,
            collection_name=COLLECTION_NAME,
            model=MODEL,
            behavior_distribution=BEHAVIOR_DISTRIBUTION
        )
        
        print("\n Dados do Inventário de Beck salvos com sucesso!")
        
    except Exception as e:
        print(f"\n Erro: {e}")
        raise