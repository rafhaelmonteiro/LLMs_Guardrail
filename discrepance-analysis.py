import pandas as pd

VALID_LABELS = ["valido", "invalido"]

def normalizar_status(valor):
    if pd.isna(valor):
        return None
    valor = str(valor).strip().lower()
    if valor in VALID_LABELS:
        return valor
    return None


def extrair_discrepancias(arquivo_csv):
    nome_escala = arquivo_csv.split('_')[1].split('.')[0].upper()
    df = pd.read_csv(arquivo_csv)

    # Normalização
    for col in ['gpt_status', 'deepseek_status', 'llama_status']:
        df[col] = df[col].apply(normalizar_status)

    df = df.dropna(subset=['gpt_status','deepseek_status','llama_status'])

    print(f"\n{'='*50}")
    print(f"ANÁLISE DE DIVERGÊNCIAS: {nome_escala}")
    print(f"{'='*50}")

    total = len(df)

    rigor_gpt = df[
        (df['gpt_status'] == 'invalido') &
        (df['deepseek_status'] == 'valido') &
        (df['llama_status'] == 'valido')
    ]

    rigor_deepseek = df[
        (df['deepseek_status'] == 'invalido') &
        (df['gpt_status'] == 'valido') &
        (df['llama_status'] == 'valido')
    ]

    rigor_llama = df[
        (df['llama_status'] == 'invalido') &
        (df['gpt_status'] == 'valido') &
        (df['deepseek_status'] == 'valido')
    ]

    unanimidade = (
        (df['gpt_status'] == df['deepseek_status']) &
        (df['deepseek_status'] == df['llama_status'])
    )

    divergencia_geral = df[~unanimidade]

    rigor_gpt.to_csv(f"discrepancias_rigor_gpt_{nome_escala}.csv", index=False)
    divergencia_geral.to_csv(f"discrepancias_geral_{nome_escala}.csv", index=False)

    print(f"Total de registros analisados: {total}")
    print(f"Divergências totais: {len(divergencia_geral)} ({len(divergencia_geral)/total*100:.2f}%)")
    print(f"GPT mais rigoroso sozinho: {len(rigor_gpt)}")
    print(f"DeepSeek mais rigoroso sozinho: {len(rigor_deepseek)}")
    print(f"LLaMA mais rigoroso sozinho: {len(rigor_llama)}")

    print("\nArquivos gerados:")
    print(f" - discrepancias_rigor_gpt_{nome_escala}.csv")
    print(f" - discrepancias_geral_{nome_escala}.csv")

if __name__ == "__main__":
    arquivos = ["dataframe_phq.csv", "dataframe_beck.csv", "dataframe_gad7.csv"]

    for arq in arquivos:
        try:
            extrair_discrepancias(arq)
        except Exception as e:
            print(f"Erro ao processar {arq}: {e}")