import calendar
import os
import unicodedata

import numpy as np
import pandas as pd

import parse_sistema_dat
import parse_cadic_dat

# Ano de geração da curva
ano = 2027

# Leitura do arquivo CSV de Carga PU
df_anual = pd.read_csv("carga/outputs/curva_tipica_mensal_pu_carga.csv", sep=';', decimal=',')

## Newave data for {ano}
MWmed_dict = parse_sistema_dat.get_MWmed_dict(zip_path="deck_newave_2025_12.zip", ano=ano)
print(MWmed_dict)

CAdic_dict = parse_cadic_dat.get_CAdic_dict(zip_path="deck_newave_2025_12.zip", ano=ano)
print(CAdic_dict)

output_dir = f"carga/resultados_{ano}"
os.makedirs(output_dir, exist_ok=True)

def carregar_calendario(arquivo: str = "calendario_horario_2015_2030.xlsx") -> pd.DataFrame:
    """
    Carrega o arquivo Excel com calendário horário.
    
    Args:
        arquivo: Caminho do arquivo Excel com calendário
    
    Returns:
        DataFrame com dados do calendário
    """
    # Tentar encontrar o arquivo no diretório atual ou no diretório pai
    caminho_arquivo = arquivo
    if not os.path.exists(caminho_arquivo):
        # Tentar no diretório pai
        caminho_arquivo = os.path.join("..", arquivo)
        if not os.path.exists(caminho_arquivo):
            # Tentar caminho absoluto a partir do diretório pai
            caminho_arquivo = os.path.join(os.path.dirname(os.path.dirname(__file__)), arquivo)
            if not os.path.exists(caminho_arquivo):
                raise FileNotFoundError(
                    f"Arquivo de calendário não encontrado: {arquivo}\n"
                    f"Procurado em: {os.getcwd()}, {os.path.join(os.getcwd(), '..')}, {caminho_arquivo}"
                )
    
    df_calendario = pd.read_excel(caminho_arquivo)
    
    # Converter DataHora para datetime se necessário
    if 'DataHora' in df_calendario.columns:
        df_calendario['DataHora'] = pd.to_datetime(df_calendario['DataHora'])
    
    return df_calendario


def remover_acentos(texto: str) -> str:
    """
    Remove acentos de uma string.
    
    Args:
        texto: String com possíveis acentos
    
    Returns:
        String sem acentos
    """
    if pd.isna(texto):
        return texto
    
    # Normalizar para NFD (decomposição) e remover caracteres diacríticos
    texto_normalizado = unicodedata.normalize('NFD', str(texto))
    texto_sem_acentos = ''.join(
        char for char in texto_normalizado 
        if unicodedata.category(char) != 'Mn'
    )
    return texto_sem_acentos


def converter_dia_semana_excel_para_python(dia_semana_excel: int) -> int:
    """
    Converte numeração de dia da semana do Excel para Python.
    
    Excel: segunda=1, terça=2, ..., domingo=7
    Python: segunda=0, terça=1, ..., domingo=6
    
    Args:
        dia_semana_excel: Dia da semana no formato Excel (1-7)
    
    Returns:
        Dia da semana no formato Python (0-6)
    """
    # Converter de 1-7 para 0-6
    return (dia_semana_excel - 1) % 7


def encontrar_semana_tipica(df_calendario: pd.DataFrame, mes: int, ano: int) -> pd.DataFrame:
    """
    Encontra a semana típica (semana com menos feriados ou nenhum) dentro de um mês específico.
    
    Args:
        df_calendario: DataFrame com dados do calendário
        mes: Mês a ser processado (1-12)
        ano: Ano a ser processado
    
    Returns:
        DataFrame com dados da semana típica (7 dias completos)
    """
    # Garantir que temos as colunas necessárias
    if 'Flag_Feriado' not in df_calendario.columns:
        raise ValueError("Coluna 'Flag_Feriado' não encontrada no calendário")
    
    # Converter Flag_Feriado para booleano se necessário
    df_calendario = df_calendario.copy()
    if df_calendario['Flag_Feriado'].dtype == 'object':
        df_calendario['Flag_Feriado'] = df_calendario['Flag_Feriado'].str.upper().str.strip() == 'VERDADEIRO'
    
    # Garantir que temos coluna Data
    if 'Data' not in df_calendario.columns:
        if 'DataHora' in df_calendario.columns:
            df_calendario['Data'] = pd.to_datetime(df_calendario['DataHora']).dt.date
        else:
            raise ValueError("Coluna 'Data' ou 'DataHora' não encontrada no calendário")
    
    df_calendario['Data'] = pd.to_datetime(df_calendario['Data'])
    
    # Filtrar calendário para o mês e ano específicos
    df_mes = df_calendario[
        (df_calendario['Data'].dt.month == mes) & 
        (df_calendario['Data'].dt.year == ano)
    ].copy()
    
    if len(df_mes) == 0:
        raise ValueError(f"Nenhum dado encontrado no calendário para {mes:02d}/{ano}")
    
    # Agrupar por data e contar feriados por dia
    df_dias = df_mes.groupby('Data').agg({
        'Flag_Feriado': 'first',  # Pegar o primeiro valor (todos os horários do dia têm o mesmo valor)
        'DiaSemana_Num': 'first'
    }).reset_index()
    
    # Encontrar semanas completas (7 dias consecutivos)
    df_dias = df_dias.sort_values('Data').reset_index(drop=True)
    
    melhor_semana = None
    menor_num_feriados = float('inf')
    
    # Procurar semanas completas começando em segunda-feira (DiaSemana_Num = 1 no Excel)
    for i in range(len(df_dias) - 6):
        semana = df_dias.iloc[i:i+7]
        
        # Verificar se é uma semana completa começando em segunda (DiaSemana_Num = 1)
        if semana['DiaSemana_Num'].iloc[0] == 1:
            # Verificar se os dias são consecutivos (1, 2, 3, 4, 5, 6, 7)
            dias_semana = semana['DiaSemana_Num'].tolist()
            if dias_semana == [1, 2, 3, 4, 5, 6, 7]:
                num_feriados = semana['Flag_Feriado'].sum()
                
                if num_feriados < menor_num_feriados:
                    menor_num_feriados = num_feriados
                    melhor_semana = semana
    
    if melhor_semana is None:
        # Se não encontrou semana começando em segunda, não retorna nada (semana tem que começar na segunda)
        raise ValueError(
            f"Não foi possível encontrar uma semana típica começando em segunda-feira "
            f"no calendário para {mes:02d}/{ano}"
        )
    
    # Filtrar o calendário completo para a semana típica
    datas_semana = melhor_semana['Data'].tolist()
    df_semana_tipica = df_mes[
        pd.to_datetime(df_mes['Data']).dt.date.isin([d.date() for d in datas_semana])
    ].copy()
    
    print(f"✓ Semana típica encontrada para {mes:02d}/{ano}: {datas_semana[0].date()} a {datas_semana[-1].date()}")
    print(f"  Número de feriados: {menor_num_feriados}")
    
    return df_semana_tipica


def adicionar_colunas_calendario(
    df_carga: pd.DataFrame,
    df_semana_tipica: pd.DataFrame
) -> pd.DataFrame:
    """
    Adiciona colunas do calendário (DataHora, Flag_Feriado, Patamar) ao DataFrame de carga.
    
    Args:
        df_carga: DataFrame com dados de carga
        df_semana_tipica: DataFrame com dados da semana típica do calendário
    
    Returns:
        DataFrame de carga com colunas adicionais do calendário
    """
    df_resultado = df_carga.copy()
    
    # Criar DataFrame de referência da semana típica com Tipo_Dia_Num convertido
    df_ref = df_semana_tipica.copy()
    
    # Converter DiaSemana_Num do Excel (1-7) para formato Python (0-6)
    if 'DiaSemana_Num' in df_ref.columns:
        df_ref['Tipo_Dia_Num'] = df_ref['DiaSemana_Num'].apply(converter_dia_semana_excel_para_python)
    else:
        raise ValueError("Coluna 'DiaSemana_Num' não encontrada no calendário")
    
    # Fazer merge baseado em Tipo_Dia_Num e Hora
    df_merge = df_ref[['Tipo_Dia_Num', 'Hora', 'DataHora', 'Flag_Feriado', 'Patamar']].drop_duplicates(
        subset=['Tipo_Dia_Num', 'Hora']
    )
    
    df_resultado = df_resultado.merge(
        df_merge,
        on=['Tipo_Dia_Num', 'Hora'],
        how='left'
    )
    
    return df_resultado


# Carregar calendário uma vez
print("📅 Carregando calendário...")
df_calendario = carregar_calendario()

### 2 ### Projeção de Carga MW com (Mean-Std)*(MWmed)
for mes in range(1, 13):
    print(f"\n🔍 Processando mês {mes:02d}/{ano}...")
    # Encontrar semana típica para este mês específico
    df_semana_tipica = encontrar_semana_tipica(df_calendario, mes, ano)
    df = df_anual.loc[
        df_anual["Mes"] == mes,
        ["Mes", "Tipo_Dia_Num", "Hora"]
    ].copy()

    nome_mes = calendar.month_abbr[mes].capitalize()

    for sub in ["SE", "S", "NE", "N"]:
        df_ss = df_anual.loc[
            df_anual["Mes"] == mes,
            [f"{sub}_pu_mean"]
        ].copy()
        df_ss[sub] = (
            (df_ss[f"{sub}_pu_mean"]) * (MWmed_dict[sub][mes] + CAdic_dict[sub][mes])
        )
        df = pd.concat([df, df_ss[sub]], axis=1)
    
    # Adicionar colunas do calendário (DataHora, Flag_Feriado, Patamar)
    df = adicionar_colunas_calendario(df, df_semana_tipica)
    
    # Reordenar colunas: Mes, Tipo_Dia_Num, Hora, DataHora, Flag_Feriado, Patamar, depois as cargas
    colunas_carga = [col for col in df.columns if col in ["SE", "S", "NE", "N"]]
    colunas_ordenadas = ["Mes", "Tipo_Dia_Num", "Hora", "DataHora", "Flag_Feriado", "Patamar"] + colunas_carga
    df = df[[col for col in colunas_ordenadas if col in df.columns]]
    
    # Normalizar colunas de texto removendo acentos
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(remover_acentos)
    
    # Saída em CSV na pasta resultados_{ano}
    nome_arquivo = os.path.join(output_dir, f"forecast_carga_{mes:02.0f}-{ano}.csv")
    df.to_csv(nome_arquivo, index=False, sep=";", decimal=",", encoding='utf-8-sig')
    print(f"✓ Arquivo salvo: {nome_arquivo}")
