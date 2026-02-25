import pandas as pd
import matplotlib.pyplot as plt

VALID_LABELS = ["valido", "invalido"]

arquivos = ["dataframe_phq.csv", "dataframe_beck.csv", "dataframe_gad7.csv"]

def normalizar_status(valor):
    if pd.isna(valor):
        return None
    valor = str(valor).strip().lower()
    if valor in VALID_LABELS:
        return valor
    return None

def percentagemAnalysis(arquivo):
    modelo = arquivo.split('_')[1].split('.')[0].upper()
    df = pd.read_csv(arquivo)

    # Normalização consistente com script de concordância
    for col in ['gpt_status', 'deepseek_status', 'llama_status']:
        df[col] = df[col].apply(normalizar_status)

    # Remover registros inválidos
    df = df.dropna(subset=['gpt_status', 'deepseek_status', 'llama_status'])

    total = len(df)

    print(f"\n{'='*50}")
    print(f"Resumo Percentual - Questionário {modelo}")
    print(f"Registros analisados: {total}")
    print(f"{'='*50}")

    resultados = []

    for col in ['gpt_status', 'deepseek_status', 'llama_status']:
        counts = df[col].value_counts(normalize=True) * 100
        
        resultados.append({
            'Modelo': col.replace('_status', '').upper(),
            'Válido (%)': counts.get('valido', 0),
            'Inválido (%)': counts.get('invalido', 0)
        })

    df_resumo = pd.DataFrame(resultados)

    print(df_resumo.to_string(index=False))

    # Gráfico
    df_plot = df_resumo.set_index('Modelo')
    ax = df_plot.plot(kind='bar', stacked=True, figsize=(8,6))

    plt.title(f'Integridade por Modelo - {modelo}')
    plt.ylabel('Percentual (%)')
    plt.xticks(rotation=0)

    for p in ax.patches:
        width, height = p.get_width(), p.get_height()
        x, y = p.get_xy()
        if height > 0:
            ax.annotate(f'{height:.1f}%', 
                        (x + width/2, y + height/2), 
                        ha='center', va='center',
                        color='white', fontweight='bold')

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    for arq in arquivos:
        percentagemAnalysis(arq)