"""
Processamento de Curvas Típicas de Carga do Sistema Interligado Nacional (SIN)

Este módulo processa dados históricos de carga horária dos subsistemas do SIN,
criando curvas típicas normalizadas por mês e tipo de dia.

Principais funcionalidades:
- Leitura e consolidação de dados de carga horária
- Remoção de feriados e outliers
- Normalização de carga em valores por unidade (pu)
- Geração de curvas típicas com média e desvio padrão

Outputs:
- dados_carga_2015-2025.csv: Dados históricos processados
- curva_tipica_mensal_pu.csv: Curvas típicas por mês e tipo de dia

TODO: Refatorar calendário para ter tudo
"""

# Bibliotecas padrão do Python
import glob
import locale
import os
import sys
from pathlib import Path
from typing import List

# Bibliotecas de terceiros
import pandas as pd

# Adicionar diretório pai ao sys.path para encontrar o módulo utils
sys.path.insert(0, str(Path(__file__).parent.parent))

# Imports locais do projeto
from utils import utils_aws, utils_snowflake

# Configuração de localização para português brasileiro
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except locale.Error:
    # Fallback se pt_BR.UTF-8 não estiver disponível
    try:
        locale.setlocale(locale.LC_TIME, "pt_BR")
    except locale.Error:
        # Usa locale padrão do sistema
        print("⚠️ Aviso: Locale pt_BR não disponível, usando locale padrão do sistema")
        pass

# Constantes do projeto
SUBSISTEMAS = ["N", "NE", "S", "SE"]
DATA_LIMITE = "2025-10-31"
ARQUIVO_SAIDA_DADOS = "dados_carga_2015-2025.csv"
ARQUIVO_SAIDA_CURVA = "curva_tipica_mensal_pu_carga.csv"

# Configuração do Snowflake
SystemsManager = utils_aws.SSM()
snowflake_credentials = SystemsManager.get_parameter(name='/snowflake/automationservice/rsa',decrypted=False)
Snowflake = utils_snowflake.SnowflakeSession(snowflake_credentials=snowflake_credentials)
Snowflake.connect()

# def get_data_from_snowflake(table_name: str, data_inicio: str, data_fim: str) -> pd.DataFrame:
#     """
#     Pega dados da tabela Snowflake especificada.
    
#     Args:
#         table_name: Nome completo da tabela (ex: COMERCIALIZACAO_PROD.MIDDLE_PRECO.F_CURVA_CARGA_HORARIA)
#         data_inicio: Data de início no formato 'YYYY-MM-DD'
#         data_fim: Data de fim no formato 'YYYY-MM-DD'
    
#     Returns:
#         DataFrame com dados da tabela de carga horária
#     """
#     query = f"""
#         SELECT * FROM {table_name} 
#         WHERE din_instante >= '{data_inicio}' 
#         AND din_instante <= '{data_fim}'
#     """
#     df = Snowflake.query_to_dataframe(query)
    
#     # Debug: mostrar colunas retornadas
#     print(f"📊 Colunas retornadas do Snowflake: {list(df.columns)}")
#     print(f"📏 Total de registros: {len(df):,}")
    
#     # Snowflake geralmente retorna colunas em MAIÚSCULAS, converter para minúsculas
#     df.columns = df.columns.str.lower()
    
#     # Garantir que din_instante está em formato datetime
#     if 'din_instante' in df.columns:
#         df['din_instante'] = pd.to_datetime(df['din_instante'])
    
#     # Converter coluna de carga para numérico (Snowflake pode retornar como string)
#     # Usar float64 para manter precisão igual ao código original
#     if 'val_cargaenergiahomwmed' in df.columns:
#         df['val_cargaenergiahomwmed'] = pd.to_numeric(df['val_cargaenergiahomwmed'], errors='coerce').astype('float64')
#         print(f"✅ Coluna de carga convertida para numérico")
    
#     return df

def get_curve_from_csv(arquivo: str = "dados_consolidados_curva_carga_ons.csv") -> pd.DataFrame:
    """
    Lê arquivo CSV com dados de carga horária.
    
    Args:
        arquivo: Caminho do arquivo CSV com dados de carga horária
    
    Returns:
        DataFrame com dados de carga horária
    """
    if not os.path.exists(arquivo):
        raise FileNotFoundError(
            f"Arquivo de carga horária não encontrado: {arquivo}\n"
            f"Diretório atual: {os.getcwd()}\n"
            f"Certifique-se de que o arquivo existe no diretório de execução."
        )
    
    df = pd.read_csv(arquivo, sep=';', decimal=',', encoding='utf-8-sig')
    
    # Debug: mostrar colunas retornadas
    print(f"📊 Colunas retornadas do CSV: {list(df.columns)}")
    print(f"📏 Total de registros: {len(df):,}")
    
    # Converter colunas para minúsculas (caso venham em maiúsculas)
    df.columns = df.columns.str.lower()
    
    # Garantir que din_instante está em formato datetime
    if 'din_instante' in df.columns:
        df['din_instante'] = pd.to_datetime(df['din_instante'])
        print(f"✅ Coluna din_instante convertida para datetime")
    
    # Converter coluna de carga para numérico (usar float64 para manter precisão)
    if 'val_cargaenergiahomwmed' in df.columns:
        df['val_cargaenergiahomwmed'] = pd.to_numeric(df['val_cargaenergiahomwmed'], errors='coerce').astype('float64')
        print(f"✅ Coluna de carga convertida para numérico")
    
    return df

def ler_feriados(arquivo: str = "feriados_db_anbima.xlsx") -> pd.DataFrame:
    """
    Lê arquivo Excel com datas de feriados.
    
    Args:
        arquivo: Caminho do arquivo Excel com feriados
    
    Returns:
        DataFrame com coluna 'Data' contendo as datas dos feriados
    """
    if not os.path.exists(arquivo):
        raise FileNotFoundError(
            f"Arquivo de feriados não encontrado: {arquivo}\n"
            f"Diretório atual: {os.getcwd()}\n"
            f"Certifique-se de que o arquivo existe no diretório de execução."
        )
    
    feriados = pd.read_excel(arquivo)
    feriados["Data"] = pd.to_datetime(feriados["Data"])
    return feriados


def pivotar_por_subsistema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma o DataFrame para ter uma coluna por subsistema.
    
    Args:
        df: DataFrame com dados de carga em formato longo
    
    Returns:
        DataFrame pivotado com colunas por subsistema
    """
    # Debug: verificar valores únicos de id_subsistema
    print(f"📊 Subsistemas únicos no DataFrame: {sorted(df['id_subsistema'].unique())}")
    
    df_pivot = df.pivot(
        index='din_instante',
        columns='id_subsistema',
        values='val_cargaenergiahomwmed'
    )
    df_pivot.columns.name = None
    df_pivot = df_pivot.reset_index()
    
    print(f"📊 Colunas após pivot: {list(df_pivot.columns)}")
    
    # Verificar se as colunas dos subsistemas existem e converter para numérico
    # Usar float64 para manter precisão igual ao código original
    for col in df_pivot.columns:
        if col != 'din_instante':
            # Converter todas as colunas (exceto din_instante) para numérico float64
            df_pivot[col] = pd.to_numeric(df_pivot[col], errors='coerce').astype('float64')
    
    # Verificar quais subsistemas esperados estão presentes
    subsistemas_presentes = [col for col in SUBSISTEMAS if col in df_pivot.columns]
    print(f"📊 Subsistemas esperados presentes: {subsistemas_presentes}")
    
    if subsistemas_presentes:
        print(f"📊 Tipos de dados dos subsistemas: {df_pivot[subsistemas_presentes].dtypes.to_dict()}")
    
    return df_pivot


def adicionar_sin_e_colunas_auxiliares(df: pd.DataFrame):
    """
    Adiciona coluna SIN (soma dos subsistemas) e colunas auxiliares de data/hora.
    
    Args:
        df: DataFrame pivotado com dados por subsistema
    
    Returns:
        tuple: (DataFrame com coluna SIN e colunas auxiliares, lista de subsistemas disponíveis)
    """
    # Identificar colunas de subsistemas (excluindo din_instante e outras colunas auxiliares)
    subsistemas_disponiveis = [col for col in df.columns if col in SUBSISTEMAS]
    
    if not subsistemas_disponiveis:
        # Se não encontrou com nomes esperados, usa todas as colunas numéricas exceto din_instante
        subsistemas_disponiveis = [col for col in df.columns if col != 'din_instante']
    
    print(f"📊 Calculando SIN com subsistemas: {subsistemas_disponiveis}")
    
    # Calcular SIN como soma dos subsistemas disponíveis
    # Garantir que o resultado seja float64 para manter precisão
    df["SIN"] = df[subsistemas_disponiveis].sum(axis=1).astype('float64')
    
    # Criar colunas auxiliares a partir do timestamp
    df['Data'] = df['din_instante'].dt.floor('D')
    df['Ano'] = df['din_instante'].dt.year
    df['Mes'] = df['din_instante'].dt.month
    df['Dia'] = df['din_instante'].dt.day
    df['Tipo_Dia'] = df['din_instante'].dt.strftime("%a")
    df['Tipo_Dia_Num'] = df['din_instante'].dt.weekday  # Segunda=0, Domingo=6
    df['Hora'] = df['din_instante'].dt.hour
    
    # Reordenar colunas para melhor legibilidade
    # Usar subsistemas disponíveis em vez de SUBSISTEMAS fixo
    colunas_ordenadas = [
        "din_instante", "Data", "Ano", "Mes", "Dia",
        "Tipo_Dia", "Tipo_Dia_Num", "Hora"
    ] + subsistemas_disponiveis + ["SIN"]
    
    df = df.reindex(columns=colunas_ordenadas)
    
    return df, subsistemas_disponiveis


def limpar_dados_historicos(df: pd.DataFrame, feriados: pd.DataFrame, subsistemas: list,
                            data_limite: str = DATA_LIMITE) -> pd.DataFrame:
    """
    Remove feriados, dados após data limite e outliers (valores não positivos).
    
    Args:
        df: DataFrame com dados de carga
        feriados: DataFrame com datas de feriados
        subsistemas: Lista de subsistemas disponíveis
        data_limite: Data limite para considerar dados históricos
    
    Returns:
        DataFrame limpo sem feriados, outliers e dados futuros
    """
    # Remover feriados
    idx_feriados = df.loc[df["Data"].isin(feriados["Data"])].index
    df_limpo = df.drop(index=idx_feriados)
    
    # Remover dados após data limite
    df_limpo = df_limpo[df_limpo["Data"] <= pd.to_datetime(data_limite)]
    
    # Remover outliers (valores não positivos que podem indicar erros ou blecautes)
    colunas_carga = subsistemas + ["SIN"]
    df_limpo = df_limpo[(df_limpo[colunas_carga] > 0).all(axis=1)]
    
    return df_limpo


def calcular_carga_normalizada(df: pd.DataFrame, subsistemas: list) -> pd.DataFrame:
    """
    Calcula carga normalizada (em pu) dividindo pela média mensal.
    
    A normalização é feita dividindo cada valor pela média mensal do respectivo
    subsistema, resultando em valores por unidade (pu).
    
    Args:
        df: DataFrame com dados de carga
        subsistemas: Lista de subsistemas disponíveis
    
    Returns:
        DataFrame com colunas adicionais de carga normalizada (_pu)
    """
    df = df.copy()
    
    # Calcular carga normalizada para cada subsistema
    # Manter precisão float64 igual ao código original
    for col in subsistemas + ["SIN"]:
        media_mensal = df.groupby(["Ano", "Mes"])[col].transform("mean").astype('float64')
        df[f"{col}_pu"] = (df[col] / media_mensal).astype('float64')
    
    return df


def adicionar_media_mensal(df: pd.DataFrame, subsistemas: list) -> pd.DataFrame:
    """
    Adiciona colunas com a média mensal (MWmed) de cada subsistema.
    
    Args:
        df: DataFrame com dados de carga e colunas _pu
        subsistemas: Lista de subsistemas disponíveis
    
    Returns:
        DataFrame com colunas adicionais _MWmed
    """
    # Calcular média mensal por subsistema
    # Manter precisão float64 igual ao código original
    mwmed_mensal = (
        df.groupby(["Ano", "Mes"])[subsistemas + ["SIN"]]
        .mean()
        .reset_index()
        .rename_axis(None, axis=1)
    )
    # Garantir que todas as colunas numéricas sejam float64
    for col in subsistemas + ["SIN"]:
        if col in mwmed_mensal.columns:
            mwmed_mensal[col] = mwmed_mensal[col].astype('float64')
    
    # Fazer merge com os dados originais
    df_final = pd.merge(
        df,
        mwmed_mensal,
        on=["Ano", "Mes"],
        suffixes=("", "_MWmed")
    )
    
    return df_final


def salvar_csv_com_backup(df: pd.DataFrame, arquivo: str, 
                          sep: str = ";", decimal: str = ",") -> None:
    """
    Salva DataFrame em CSV, removendo arquivo anterior se existir.
    
    Args:
        df: DataFrame a ser salvo
        arquivo: Nome do arquivo de saída
        sep: Separador de colunas
        decimal: Separador decimal
    """
    if os.path.exists(arquivo):
        os.remove(arquivo)
    
    df.to_csv(arquivo, sep=sep, decimal=decimal, index=False)


def criar_curva_tipica(df: pd.DataFrame, subsistemas: list) -> pd.DataFrame:
    """
    Cria curva típica agregando por mês, tipo de dia e hora.
    
    Calcula média e desvio padrão da carga normalizada (pu) para cada combinação
    de mês, tipo de dia (seg-dom) e hora.
    
    Args:
        df: DataFrame com dados de carga normalizada (_pu)
        subsistemas: Lista de subsistemas disponíveis
    
    Returns:
        DataFrame com curva típica contendo média e desvio padrão
    """
    # Selecionar apenas colunas necessárias (dados em pu)
    colunas_pu = [f"{col}_pu" for col in subsistemas + ["SIN"]]
    colunas_grupo = ["Mes", "Tipo_Dia_Num", "Hora"]
    
    # Criar dicionário de agregação dinamicamente
    agg_dict = {col: ["mean", "std"] for col in colunas_pu}
    
    # Agrupar e agregar
    curva_tipica = (
        df[colunas_grupo + colunas_pu]
        .groupby(colunas_grupo)
        .agg(agg_dict)
    )
    
    # Renomear colunas para formato plano
    curva_tipica.columns = [f"{col}_{stat}" for col, stat in curva_tipica.columns]
    curva_tipica = curva_tipica.reset_index()
    
    # Garantir que todas as colunas numéricas sejam float64 para manter precisão
    for col in curva_tipica.columns:
        if col not in colunas_grupo:
            curva_tipica[col] = curva_tipica[col].astype('float64')
    
    return curva_tipica


def main() -> None:
    """
    Função principal que executa o pipeline completo de processamento.
    
    Etapas:
    1. Leitura dos dados de carga e feriados
    2. Pivotagem por subsistema e adição de colunas auxiliares
    3. Limpeza de dados (remoção de feriados, outliers e dados futuros)
    4. Normalização da carga (cálculo em pu)
    5. Adição de médias mensais
    6. Geração e salvamento da curva típica
    """
    print("Iniciando processamento de curvas típicas de carga...")
    
    # Etapa 1: Leitura dos dados
    print("\n📂 Lendo arquivos de carga...")
    df_total = get_curve_from_csv(arquivo="dados_consolidados_curva_carga_ons.csv")
    
    print("📂 Lendo arquivo de feriados...")
    feriados = ler_feriados()
    
    # Etapa 2: Transformação e pivotagem
    print("\n🔄 Pivotando dados por subsistema...")
    df_pivot = pivotar_por_subsistema(df_total)
    df_pivot, subsistemas_disponiveis = adicionar_sin_e_colunas_auxiliares(df_pivot)
    
    # Etapa 3: Limpeza dos dados
    print("🧹 Limpando dados históricos (removendo feriados e outliers)...")
    df_hist = limpar_dados_historicos(df_pivot, feriados, subsistemas_disponiveis)
    
    # Etapa 4: Normalização
    print("📊 Calculando carga normalizada (pu)...")
    df_hist = calcular_carga_normalizada(df_hist, subsistemas_disponiveis)
    
    # Etapa 5: Adicionar médias mensais
    print("📈 Adicionando médias mensais...")
    df_final = adicionar_media_mensal(df_hist, subsistemas_disponiveis)
    
    # Etapa 6: Salvar dados processados
    print(f"\n💾 Salvando arquivo {ARQUIVO_SAIDA_DADOS}...")
    salvar_csv_com_backup(df_final, ARQUIVO_SAIDA_DADOS)
    print(f"✅ Arquivo criado com sucesso. Total de registros: {len(df_final):,}")
    
    # Etapa 7: Criar e salvar curva típica
    print(f"\n📉 Criando curva típica mensal...")
    curva_tipica = criar_curva_tipica(df_final, subsistemas_disponiveis)
    
    print(f"💾 Salvando arquivo {ARQUIVO_SAIDA_CURVA}...")
    salvar_csv_com_backup(curva_tipica, ARQUIVO_SAIDA_CURVA)
    print(f"✅ Curva típica criada com sucesso. Total de registros: {len(curva_tipica):,}")
    
    print("\n🎉 Processamento concluído!")


if __name__ == "__main__":
    main()