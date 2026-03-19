import pandas as pd
import numpy as np
import ast
import matplotlib.pyplot as plt
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    f1_score, confusion_matrix, ConfusionMatrixDisplay
)

LIKERT_VALIDAS = {
    'Nenhuma vez', 'Vários dias',
    'Mais da metade dos dias', 'Quase todos os dias'
}

BECK_VALIDAS_POR_QUESTAO = {
    1:  {'Eu me sinto triste', 'Estou sempre triste e não consigo sair disto'},
    2:  {'Eu me sinto desanimado quanto ao futuro', 'Acho que nada tenho a esperar'},
    3:  {'Quando olho para trás, na minha vida, tudo o que posso ver é um monte de fracassos', 'Acho que fracassei mais do que uma pessoa comum'},
    4:  {'Não encontro um prazer real em mais nada', 'Não sinto mais prazer nas coisas como antes'},
    5:  {'Eu me sinto culpado às vezes', 'Eu me sinto culpado na maior parte do tempo'},
    6:  {'Acho que posso ser punido', 'Creio que vou ser punido'},
    7:  {'Estou decepcionado comigo mesmo', 'Estou enojado de mim'},
    8:  {'Eu me culpo sempre por minhas falhas', 'Sou crítico em relação a mim por minhas fraquezas ou erros'},
    9:  {'Não tenho quaisquer ideias de me matar', 'Tenho ideias de me matar, mas não as executaria', 'Gostaria de me matar', 'Eu me mataria se tivesse oportunidade'},
    10: {'Choro mais agora do que costumava', 'Agora, choro o tempo todo'},
    11: {'Fico aborrecido ou irritado mais facilmente do que costumava', 'Atualmente me sinto irritado o tempo todo'},
    12: {'Interesso-me menos do que costumava pelas outras pessoas', 'Perdi a maior parte do meu interesse nas outras pessoas'},
    13: {'Tenho maior dificuldade em tomar decisões do que antes', 'Adio minhas decisões mais do que costumava'},
    14: {'Preocupo-me por estar parecendo velho ou sem atrativos', 'Sinto que há mudanças permanentes em minha aparência que me fazem parecer sem atrativos'},
    15: {'Preciso de um esforço extra para começar qualquer coisa', 'Tenho de me esforçar muito até fazer qualquer coisa'},
    16: {'Não durmo tão bem quanto costumava', 'Acordo uma ou duas horas mais cedo do que de costume e tenho dificuldade para voltar a dormir'},
    17: {'Sinto-me cansado ao fazer quase qualquer coisa', 'Fico cansado com mais facilidade do que costumava'},
    18: {'Meu apetite não é tão bom quanto costumava ser', 'Meu apetite está muito pior agora'},
    19: {'Não perdi muito peso, se é que perdi algum ultimamente', 'Perdi mais de 2,5 Kg', 'Perdi mais de 5,0 Kg'},
    20: {'Preocupo-me com problemas físicos como dores e aflições, mal-estar no estômago ou prisão de ventre', 'Estou muito preocupado com problemas físicos e é difícil pensar em outra coisa'},
    21: {'Estou bem menos interessado em sexo atualmente', 'Estou menos interessado por sexo do que costumava'},
}

MODELOS = ['gpt_status', 'deepseek_status', 'llama_status']
ESCALAS = {
    "dataframe_phq.csv":  {"nome": "PHQ-9",  "metodo_gt": "likert"},
    "dataframe_beck.csv": {"nome": "BDI-II", "metodo_gt": "beck"},
    "dataframe_gad7.csv": {"nome": "GAD-7",  "metodo_gt": "likert"},
}

def ground_truth_likert(df):
    def verificar(respostas_str):
        try:
            respostas = ast.literal_eval(respostas_str)
            return 'valido' if all(r['resposta'].strip() in LIKERT_VALIDAS for r in respostas) else 'invalido'
        except Exception:
            return 'invalido'
    return df['respostas'].apply(verificar)

def ground_truth_beck(df):
    def verificar(respostas_str):
        try:
            respostas = ast.literal_eval(respostas_str)
            for r in respostas:
                if r['resposta'].strip() not in BECK_VALIDAS_POR_QUESTAO.get(r['numero_questao'], set()):
                    return 'invalido'
            return 'valido'
        except Exception:
            return 'invalido'
    return df['respostas'].apply(verificar)

def calcular_metricas(arquivo, config):
    nome   = config["nome"]
    metodo = config["metodo_gt"]
    df = pd.read_csv(arquivo)
    df = df.dropna(subset=MODELOS)

    if metodo == "likert":
        df['ground_truth'] = ground_truth_likert(df)
        descricao_gt = "Opções Likert válidas do instrumento (verificação por questão)"
    else:
        df['ground_truth'] = ground_truth_beck(df)
        descricao_gt = "Opções válidas do BDI-II por questão (verificação programática)"

    binarizar = lambda s: s.map({'valido': 1, 'invalido': 0})
    y_true = binarizar(df['ground_truth'])

    print(f"\n{'='*60}")
    print(f" {nome} — {descricao_gt}")
    print(f" Total: {len(df)} | Válidos GT: {(df['ground_truth']=='valido').sum()} | Inválidos GT: {(df['ground_truth']=='invalido').sum()}")
    print(f"{'='*60}")
    print(f"{'Modelo':<12} {'Accuracy':>10} {'Precision':>10} {'Recall':>10} {'F1-Score':>10}")
    print(f"{'-'*52}")

    resultados = {}
    for col in MODELOS:
        nm = col.replace('_status', '').upper()
        y_pred = binarizar(df[col])
        m = {
            'accuracy':  accuracy_score(y_true, y_pred),
            'precision': precision_score(y_true, y_pred, zero_division=0),
            'recall':    recall_score(y_true, y_pred, zero_division=0),
            'f1':        f1_score(y_true, y_pred, zero_division=0),
            'cm':        confusion_matrix(y_true, y_pred),
        }
        resultados[nm] = m
        print(f"{nm:<12} {m['accuracy']:>10.3f} {m['precision']:>10.3f} {m['recall']:>10.3f} {m['f1']:>10.3f}")

    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    fig.suptitle(f'Matrizes de Confusão — {nome}', fontsize=13, fontweight='bold', y=1.02)
    for ax, (nm, m) in zip(axes, resultados.items()):
        ConfusionMatrixDisplay(m['cm'], display_labels=['Inválido', 'Válido']).plot(ax=ax, colorbar=False, cmap='Blues')
        ax.set_title(nm, fontsize=12, fontweight='bold')
        ax.set_xlabel(f"Acc={m['accuracy']:.2f}  Prec={m['precision']:.2f}  Rec={m['recall']:.2f}  F1={m['f1']:.2f}", fontsize=9, labelpad=10)
    plt.tight_layout()
    plt.savefig(f"metricas_{nome.replace('-','')}.png", dpi=150, bbox_inches='tight')
    plt.show()
    print(f"→ Figura salva: metricas_{nome.replace('-','')}.png")
    return resultados

for arquivo, config in ESCALAS.items():
    try:
        calcular_metricas(arquivo, config)
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {arquivo}")
    except Exception as e:
        print(f"Erro em {arquivo}: {e}")