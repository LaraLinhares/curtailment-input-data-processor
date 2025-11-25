import pandas as pd
import numpy as np
import locale
import os
import re

# Tentar definir locale para português brasileiro
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, "pt_BR")
    except locale.Error:
        print("⚠️ Aviso: Locale pt_BR não disponível, usando locale padrão do sistema")

### Dataframe histórico completo
# Ler CSV e converter colunas numéricas que possam usar vírgula como separador decimal
df_total = pd.read_csv("dados_consolidados_geracao_usina_2_ho.csv", sep=';')
# Converter coluna numérica se necessário
if 'val_geracao' in df_total.columns:
    df_total['val_geracao'] = pd.to_numeric(
        df_total['val_geracao'].astype(str).str.replace(',', '.'), 
        errors='coerce'
    )
df_total['din_instante'] = pd.to_datetime(df_total['din_instante'])     # Garantir formato data/hora

# Aplicar filtros da query SQL de referência:
# 1. Filtrar por modalidade de operação (excluir TIPO I e TIPO II-A - não despachadas centralizadamente)
# Verificar se a coluna existe (pode ter nomes diferentes)
col_modalidade = None
for col in df_total.columns:
    if 'modalidade' in col.lower() or 'cod_modalidade' in col.lower():
        col_modalidade = col
        break

if col_modalidade:
    print(f"📋 Aplicando filtro de modalidade de operação (coluna: {col_modalidade})")
    # Excluir TIPO I e TIPO II-A (não despachadas centralizadamente)
    df_total = df_total[~df_total[col_modalidade].isin(['TIPO I', 'TIPO II-A'])]
    print(f"   Registros após filtro: {len(df_total):,}")
else:
    print("⚠️ Aviso: Coluna de modalidade de operação não encontrada. Filtro não aplicado.")

# 2. Filtrar por período de data (se necessário)
# A query SQL usa DATA_INICIO e DATA_FIM, mas o CSV já pode estar filtrado
# Se necessário, adicionar filtro aqui

# Dados auxiliares, cálculo de MWmed e dados PU
df_final = []
for fonte in ["HIDROELÉTRICA","TÉRMICA"]:
    df_fonte = df_total[df_total["nom_tipousina"] == fonte]

    # Pivotar para ter colunas por subsistema
    # Usar pivot_table com agregação para lidar com valores duplicados
    # Se houver múltiplos valores para o mesmo din_instante e id_subsistema, somar
    df_pivot = df_fonte.pivot_table(
        index='din_instante', 
        columns='id_subsistema', 
        values='val_geracao',
        aggfunc='sum'  # Somar valores duplicados (sem fill_value para não preencher com zero)
    )
    df_pivot.columns.name = None
    df_pivot = df_pivot.reset_index() # Adicionar din_instante como coluna
    
    # Mapear IDs de subsistema para nomes (1=SE, 2=S, 3=NE, 4=N)
    mapeamento_subsistemas = {1: "SE", 2: "S", 3: "NE", 4: "N"}
    df_pivot = df_pivot.rename(columns=mapeamento_subsistemas)
    
    # Garantir que todas as colunas de subsistemas existam (preencher com NaN se não existirem)
    for sub in ["N", "NE", "S", "SE"]:
        if sub not in df_pivot.columns:
            df_pivot[sub] = pd.NA
    
    # Calcular SIN como soma de todos os subsistemas (após renomear)
    subsistemas_cols = [col for col in df_pivot.columns if col in ["N", "NE", "S", "SE"]]
    if subsistemas_cols:
        df_pivot['SIN'] = df_pivot[subsistemas_cols].sum(axis=1, skipna=True)

    # Criar colunas auxiliares
    df_pivot['Data'] = df_pivot['din_instante'].dt.floor('D')
    df_pivot['Ano'] = df_pivot['din_instante'].dt.year
    df_pivot['Mes'] = df_pivot['din_instante'].dt.month
    df_pivot['Dia'] = df_pivot['din_instante'].dt.day
    df_pivot['Tipo_Dia'] = df_pivot['din_instante'].dt.strftime("%a")
    df_pivot['Tipo_Dia_Num'] = df_pivot['din_instante'].dt.weekday  # Seg=0, Dom=6
    df_pivot['Hora'] = df_pivot['din_instante'].dt.hour
    if fonte == "HIDROELÉTRICA":
        df_pivot['TIPO_GERACAO'] = "PCH"
    elif fonte == "TÉRMICA":
        df_pivot['TIPO_GERACAO'] = "PCT"

    # Reordem de colunas de Dataframe
    df_pivot = df_pivot.reindex(
        columns=["TIPO_GERACAO","din_instante",
        "Data","Ano","Mes","Dia","Tipo_Dia","Tipo_Dia_Num","Hora",
        "N","NE", "S", "SE", "SIN"]
        )

    # Cálculo da geração normalizada pelo MWmed mensal
    for col in ["N","NE","S","SE","SIN"]:
        df_pivot[f"{col}_pu"] = df_pivot[col] / df_pivot.groupby(["Ano","Mes"])[col].transform("mean")
    df_pivot = df_pivot.fillna(0)

    # Cálculo da média mensal por subsistema
    mwmed_mensal = (
        df_pivot.groupby(["Ano", "Mes"])[["N", "NE", "S", "SE", "SIN"]]
        .mean()
        .reset_index()
        .rename_axis(None, axis=1)
    )

    ### Dataframe completo
    df_completo = pd.merge(
        df_pivot,
        mwmed_mensal,
        on=["Ano", "Mes"],
        suffixes=("", "_MWmed")  # as colunas médias virão com sufixo _MWmed
    )
    df_final.append(df_completo)
    
# Salvar em CSV
if os.path.exists("dados_PCH-PCT_2018-2025.csv"):
    os.remove("dados_PCH-PCT_2018-2025.csv")      # Remove arquivo antigo, se existir
df_final = pd.concat(df_final, ignore_index=True)
# Salvar CSV e converter ponto para vírgula nos valores numéricos
df_final.to_csv("dados_PCH-PCT_2018-2025.csv", sep=";", index=False)
# Converter ponto para vírgula nos valores numéricos (pós-processamento)
with open("dados_PCH-PCT_2018-2025.csv", 'r', encoding='utf-8') as f:
    conteudo = f.read()
conteudo = re.sub(r'(\d+)\.(\d+)', r'\1,\2', conteudo)
with open("dados_PCH-PCT_2018-2025.csv", 'w', encoding='utf-8') as f:
    f.write(conteudo)
print(f"✅ Arquivo dados_PCH-PCT_2018-2025.csv criado com sucesso. Número total de registros: {len(df_final)}")

### Criação da curva típica em pu, por tipo de dia e mês
### Dataframe só com dados em pu
for fonte in ["PCH","PCT"]:
    df_pu = df_final[df_final["TIPO_GERACAO"] == fonte]
    df_pu = df_pu.drop(columns=["din_instante","N","NE","S","SE","SIN","N_MWmed","NE_MWmed","S_MWmed","SE_MWmed","SIN_MWmed"])

    curva_tipica = (
        df_pu
        .groupby(["Mes", "Tipo_Dia_Num", "Hora"])[["N_pu", "NE_pu", "S_pu", "SE_pu", "SIN_pu"]]
        .agg({
            "N_pu": ["mean", "std"],
            "NE_pu": ["mean", "std"],
            "S_pu": ["mean", "std"],
            "SE_pu": ["mean", "std"],
            "SIN_pu": ["mean", "std"]
        })
    )
    curva_tipica.columns = [
        f"{col}_{stat}" for col, stat in curva_tipica.columns
    ]
    curva_tipica = curva_tipica.reset_index()

    ### Guardar Dataframe por Fonte (opcional, para uso posterior)
    if fonte == "PCH":
        curva_tipica_pch = curva_tipica.copy()
    elif fonte == "PCT":
        curva_tipica_pct = curva_tipica.copy()

    # Remove arquivo antigo, se existir
    nome_arquivo = f"curva_tipica_{fonte.lower()}_mensal_pua.csv"
    if os.path.exists(nome_arquivo):
        os.remove(nome_arquivo)
    # Salvar em CSV
    curva_tipica.to_csv(nome_arquivo, sep=";", index=False)
    # Converter ponto para vírgula nos valores numéricos (pós-processamento)
    with open(nome_arquivo, 'r', encoding='utf-8') as f:
        conteudo = f.read()
    conteudo = re.sub(r'(\d+)\.(\d+)', r'\1,\2', conteudo)
    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        f.write(conteudo)
    print(f"✅ Curva típica PU para {fonte.lower()} por mês e tipo de dia criada. Registros: {len(curva_tipica)}")