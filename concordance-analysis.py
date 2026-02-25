import pandas as pd
from sklearn.metrics import cohen_kappa_score
import statsmodels.stats.inter_rater as ir
import numpy as np

arquivos = ["dataframe_phq.csv", "dataframe_beck.csv", "dataframe_gad7.csv"]

VALID_LABELS = ["valido", "invalido"]

def normalizar_status(valor):
    """
    Normaliza o status para formato padronizado.
    Retorna 'valido', 'invalido' ou None se inválido.
    """
    if pd.isna(valor):
        return None
    
    valor = str(valor).strip().lower()
    
    if valor == "valido":
        return "valido"
    elif valor == "invalido":
        return "invalido"
    else:
        return None  


def calcular_concordancia(arquivo):
    nome_escala = arquivo.split('_')[1].split('.')[0].upper()
    df = pd.read_csv(arquivo)
    
    print(f"\n{'='*50}")
    print(f"ANÁLISE DE CONCORDÂNCIA: {nome_escala}")
    print(f"{'='*50}")

    # Normalização
    for col in ['gpt_status', 'deepseek_status', 'llama_status']:
        df[col] = df[col].apply(normalizar_status)

    # Remover linhas com qualquer valor inválido
    df = df.dropna(subset=['gpt_status', 'deepseek_status', 'llama_status'])

    # Garantir apenas categorias válidas
    df = df[
        df[['gpt_status', 'deepseek_status', 'llama_status']]
        .isin(VALID_LABELS)
        .all(axis=1)
    ]

    total_registros = len(df)
    print(f"Registros válidos analisados: {total_registros}")

    if total_registros == 0:
        print("Nenhum registro válido disponível para análise.")
        return

    # 1. KAPPA DE COHEN
    kappa_gd = cohen_kappa_score(df['gpt_status'], df['deepseek_status'])
    kappa_gl = cohen_kappa_score(df['gpt_status'], df['llama_status'])
    kappa_dl = cohen_kappa_score(df['deepseek_status'], df['llama_status'])

    print("\nKappa de Cohen:")
    print(f" GPT vs DEEPSEEK : {kappa_gd:.3f}")
    print(f" GPT vs LLAMA    : {kappa_gl:.3f}")
    print(f" DEEPSEEK vs LLAMA: {kappa_dl:.3f}")

    # 2. KAPPA DE FLEISS

    cat_df = df[['gpt_status', 'deepseek_status', 'llama_status']].replace({
        'valido': 0,
        'invalido': 1
    })

    def count_votes(row):
        return [list(row).count(0), list(row).count(1)]

    vote_counts = np.array([count_votes(row) for row in cat_df.values])

    kappa_fleiss = ir.fleiss_kappa(vote_counts)

    print(f"\nKappa de Fleiss (3 modelos): {kappa_fleiss:.3f}")

    # 3. CONCORDÂNCIA UNÂNIME

    unanimidade = (
        (df['gpt_status'] == df['deepseek_status']) &
        (df['deepseek_status'] == df['llama_status'])
    )

    perc_unanime = unanimidade.mean() * 100
    print(f"Concordância Unânime: {perc_unanime:.2f}%")

    # 4. Distribuição Geral

    print("\nDistribuição Geral de Status:")
    print(df[['gpt_status','deepseek_status','llama_status']].apply(pd.Series.value_counts))


for arq in arquivos:
    try:
        calcular_concordancia(arq)
    except Exception as e:
        print(f"Erro ao processar {arq}: {e}")