import pandas as pd
import zipfile
import re
import os
from pathlib import Path
import locale
import numpy as np

# Tenta configurar o locale para Português do Brasil para garantir consistência
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR')
    except locale.Error:
        pass # Ignora se não conseguir definir

# Dicionário de mapeamento de dias da semana de inglês para português
# MANTÉM acentos!!
DAY_MAP_EN_TO_PT = {
    'Monday': 'SEGUNDA',
    'Tuesday': 'TERÇA',
    'Wednesday': 'QUARTA',
    'Thursday': 'QUINTA',
    'Friday': 'SEXTA',
    'Saturday': 'SÁBADO',
    'Sunday': 'DOMINGO'
}

# Mapeamento de dia da semana em português (sem acento) para número 0-6 (SEGUNDA=0 ... DOMINGO=6)
DAY_NAME_PT_TO_NUM = {
    'SEGUNDA': 0,
    'TERCA': 1,
    'QUARTA': 2,
    'QUINTA': 3,
    'SEXTA': 4,
    'SABADO': 5,
    'DOMINGO': 6
}

# Função utilitária para remover acentos dos dias da semana na saída do CSV
import unicodedata
def remove_acentos_dia_semana(dia):
    # Só aplica para str
    if isinstance(dia, str):
        # Especificamente remover acentos apenas de TERÇA e SÁBADO
        if dia == 'TERÇA':
            return 'TERCA'
        elif dia == 'SÁBADO':
            return 'SABADO'
        else:
            return dia
    return dia

# --- Configurações de Caminhos ---
# Diretório base onde estão todas as pastas de input
BASE_DIR = Path("/home/laral/repos/curtailment/curtailment-inputs-process/input_processor/pacote-pred-2026v2")

# Diretório para salvar os arquivos CSV combinados e melted
OUTPUT_DIR = BASE_DIR / "combined_inputs_2026"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True) # Cria o diretório se não existir

# --- Funções Auxiliares ---

def read_and_clean_data(file_path: Path, file_type: str, separator=';', cols_to_numeric=None) -> pd.DataFrame:
    """
    Lê um arquivo CSV ou Excel, convertendo valores numéricos com vírgula para ponto decimal
    e transformando-os em tipo numérico.
    """
    if file_type == 'csv':
        df = pd.read_csv(file_path, sep=separator)
    elif file_type == 'xlsx':
        df = pd.read_excel(file_path)
    else:
        raise ValueError("File type must be 'csv' or 'xlsx'")

    if cols_to_numeric:
        for col in cols_to_numeric:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def get_month_abbr(month_num: int) -> str:
    """Retorna a abreviação do mês em português (JAN, FEV, etc.) dado o número do mês."""
    month_map = {
        1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
        7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
    }
    return month_map.get(month_num, '')


def _parse_conft_dat(zfile: zipfile.ZipFile) -> pd.DataFrame:
    all_files_in_zip = zfile.namelist()
    conft_dat_files = [f for f in all_files_in_zip if os.path.basename(f).upper() == "CONFT.DAT"]
    
    if not conft_dat_files:
        print("❌ Nenhum arquivo 'CONFT.DAT' (case-insensitive, apenas nome base) encontrado no ZIP.")
        return pd.DataFrame(columns=['ID', 'SSIS'])

    conft_file_name = conft_dat_files[0]
    lines = zfile.open(conft_file_name).read().decode("latin1").splitlines()

    conft_registros = []
    conft_regex = re.compile(r"^\s*(\d+)\s+(.+?)\s+(\d+)\s+([A-Z]{2})\s+(\d+)\s*$")

    for i, line in enumerate(lines):
        if i < 2 or not line.strip(): 
            continue

        match = conft_regex.match(line)
        if not match:
            continue

        try:
            usina_id = int(match.group(1)) 
            ssis = int(match.group(3))     
            conft_registros.append({"ID": usina_id, "SSIS": ssis})
        except ValueError as e:
            print(f"⚠️ Erro de conversão para int na linha {i+1} do CONFT.DAT (após regex match): '{line.strip()}' - {e}")
            continue

    return pd.DataFrame(conft_registros)


def _parse_clast_dat(zfile: zipfile.ZipFile) -> pd.DataFrame:
    all_files_in_zip = zfile.namelist()
    clast_dat_files = [f for f in all_files_in_zip if os.path.basename(f).upper() == "CLAST.DAT"]

    if not clast_dat_files:
        print("❌ Nenhum arquivo 'CLAST.DAT' (case-insensitive, apenas nome base) encontrado no ZIP.")
        return pd.DataFrame(columns=['ID', 'TIPO_COMBUSTIVEL', 'CUSTO_CVU'])

    clast_file_name = clast_dat_files[0]
    lines = zfile.open(clast_file_name).read().decode("latin1").splitlines()

    clast_registros = []
    clast_regex = re.compile(r"^\s*(\d+)\s+(.+?)\s+(.+?)\s+(?:[\d\.]+)\s+([\d\.]+)(?:\s+[\d\.]+)*\s*$")

    start_data_line = 2 
    for i in range(start_data_line, len(lines)):
        line = lines[i]
        if line.strip() == "9999": 
            print(f"Encontrado separador '9999' na linha {i+1} de CLAST.DAT. Parando parsing desta seção.")
            break
        if not line.strip(): 
            continue

        match = clast_regex.match(line)
        if not match:
            continue

        try:
            usina_id = int(match.group(1))
            tipo_combustivel = match.group(3).strip()
            custo_cvu = float(match.group(4).replace(",", ".")) 

            clast_registros.append({
                "ID": usina_id,
                "TIPO_COMBUSTIVEL": tipo_combustivel,
                "CUSTO_CVU": custo_cvu
            })
        except ValueError as e:
            print(f"⚠️ Erro de conversão para int/float na linha {i+1} do CLAST.DAT: '{line.strip()}' - {e}")
            continue

    return pd.DataFrame(clast_registros)


def _parse_eum_data(excel_file_path: Path) -> pd.DataFrame:
    """
    Função auxiliar para ler o arquivo Excel de Eol/Ufv/Mmgd, extraindo APENAS
    os dados de MMGD, UFV e EOL, organizados por INSTANTE, HORA, TIPO DIA, CATEGORIA DIA.
    """
    print(f"\n--- Investigando '{excel_file_path}' ---")
    
    if not excel_file_path.exists():
        print(f"❌ Arquivo Excel '{excel_file_path}' não encontrado.")
        return pd.DataFrame()

    try:
        eum_df_full = read_and_clean_data(
            excel_file_path,
            'xlsx',
            cols_to_numeric=[
                'MMGD - SE', 'MMGD - S', 'MMGD - NE', 'MMGD - N',
                'UFV - SE', 'UFV - S', 'UFV - NE', 'UFV - N',
                'EOL - SE', 'EOL - S', 'EOL - NE', 'EOL - N',
            ]
        )
        print(f"✅ Arquivo Excel '{excel_file_path.name}' lido com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo Excel '{excel_file_path.name}': {e}")
        return pd.DataFrame()

    # Definir as colunas de MMGD, UFV e EOL que queremos usar do Excel EUM
    eum_desired_data_cols = [
        'MMGD - SE', 'MMGD - S', 'MMGD - NE', 'MMGD - N',
        'UFV - SE', 'UFV - S', 'UFV - NE', 'UFV - N',
        'EOL - SE', 'EOL - S', 'EOL - NE', 'EOL - N' 
    ]
    eum_key_cols = ['INSTANTE', 'TIPO DIA', 'CATEGORIA DIA', 'HORA']
    
    # Filtrar o DataFrame para conter apenas as colunas desejadas (chaves + dados)
    # Garante que só pegamos colunas que realmente existem
    eum_df = eum_df_full[[col for col in (eum_key_cols + eum_desired_data_cols) if col in eum_df_full.columns]].copy()

    # Renomear colunas para padronização e evitar conflitos
    eum_df = eum_df.rename(columns={
        'MMGD - SE': 'MMGD_SE', 'MMGD - S': 'MMGD_S', 'MMGD - NE': 'MMGD_NE', 'MMGD - N': 'MMGD_N',
        'UFV - SE': 'UFV_SE', 'UFV - S': 'UFV_S', 'UFV - NE': 'UFV_NE', 'UFV - N': 'UFV_N',
        'EOL - SE': 'EOL_SE', 'EOL - S': 'EOL_S', 'EOL - NE': 'EOL_NE', 'EOL - N': 'EOL_N'
    })

    # Criar uma coluna 'Hora_EUM_Key' para alinhamento com carga_df.Hora (0-23)
    if 'HORA' in eum_df.columns:
        eum_df['Hora_EUM_Key'] = eum_df['HORA'] - 1
        if not eum_df['Hora_EUM_Key'].between(0, 23).all():
            print(f"⚠️ Atenção: Coluna 'HORA' no Excel EUM contém valores que, após ajuste para 0-23, estão fora do esperado.")
    else:
        print("❌ Coluna 'HORA' não encontrada no Excel EUM. Incapaz de criar chave de mesclagem.")
        return pd.DataFrame()

    if not all(col in eum_df.columns for col in ['INSTANTE', 'TIPO DIA', 'CATEGORIA DIA']):
        print("❌ Colunas 'INSTANTE', 'TIPO DIA' ou 'CATEGORIA DIA' não encontradas no Excel EUM. Incapaz de criar chave de mesclagem.")
        return pd.DataFrame()

    print(f"--- Fim da Investigação de '{excel_file_path.name}' ---\n")

    # Retorna as colunas de chave e as colunas de dados de EUM (apenas MMGD, UFV, EOL)
    eum_final_cols = ['INSTANTE', 'TIPO DIA', 'CATEGORIA DIA', 'Hora_EUM_Key'] + [
        col for col in eum_df.columns if col.startswith(('MMGD_', 'UFV_', 'EOL_'))
    ]
    return eum_df[eum_final_cols]


def melt_combined_dataframe(df_combined: pd.DataFrame, month_num: int, year_str: str) -> pd.DataFrame:
    """
    Realiza a operação de 'melt' no DataFrame combinado para transformá-lo em um formato longo,
    conforme o exemplo fornecido pelo usuário.
    """
    print("Iniciando operação de 'melt' no DataFrame combinado...")

    # Definir as colunas identificadoras (id_vars) que permanecerão como colunas
    id_vars = ['DataHora', 'Hora', 'DiaDaSemana_PT', 'Flag_Feriado', 'Patamar']

    # Definir as colunas de valores (value_vars) a serem "derretidas"
    # Primeiro, as colunas de carga por submercado
    carga_cols = ['SE', 'S', 'NE', 'N']
    
    # Em seguida, as colunas de PCH, PCT, MMGD, UFV, EOL por submercado
    param_prefixes = ['PCH', 'PCT', 'MMGD', 'UFV', 'EOL']
    submercados = ['SE', 'S', 'NE', 'N']

    value_vars = carga_cols[:] # Inicia com as colunas de carga

    for prefix in param_prefixes:
        for sub in submercados:
            col_name = f"{prefix}_{sub}"
            if col_name in df_combined.columns: # Verifica se a coluna existe antes de adicionar
                value_vars.append(col_name)
    
    # Filtrar id_vars e value_vars para garantir que apenas colunas existentes sejam usadas
    id_vars = [col for col in id_vars if col in df_combined.columns]
    value_vars = [col for col in value_vars if col in df_combined.columns]

    if not value_vars:
        print("⚠️ Nenhuma coluna de valor encontrada para o 'melt'. Retornando DataFrame vazio.")
        return pd.DataFrame()

    # Realizar o melt
    df_melted = pd.melt(
        df_combined,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='original_param_submercado',
        value_name='VALOR'
    )

    # Criar as colunas 'PARAMETRO' e 'SUBMERCADO' a partir de 'original_param_submercado'
    def parse_param_submercado(col_val):
        parts = col_val.split('_')
        if len(parts) == 1: # Caso seja uma coluna de carga (e.g., 'SE', 'S')
            param = 'CARGA'
            submercado = parts[0]
        elif len(parts) == 2: # Caso seja PCH/PCT/MMGD/UFV/EOL (e.g., 'PCH_SE', 'MMGD_N')
            param = parts[0]
            submercado = parts[1]
        else: # Fallback para casos inesperados
            param = 'UNKNOWN'
            submercado = col_val # Manter o valor original para inspeção
        return param.upper(), submercado.upper() # Adicionado .upper() aqui
    
    df_melted[['PARAMETRO', 'SUBMERCADO']] = df_melted['original_param_submercado'].apply(
        lambda x: pd.Series(parse_param_submercado(x))
    )

    # Mapear SUBMERCADO para ID_SUBMERCADO
    submercado_id_map = {'SE': 1, 'S': 2, 'NE': 3, 'N': 4}
    df_melted['ID_SUBMERCADO'] = df_melted['SUBMERCADO'].map(submercado_id_map)

    # Criar a coluna ID_INPUT
    month_abbr = get_month_abbr(month_num)
    df_melted['ID_INPUT'] = f"PRED-{month_abbr}-{year_str}" # Ex: "PRED-JAN-2026"

    # Renomear as colunas para o formato final desejado
    df_melted = df_melted.rename(columns={
        'DataHora': 'TIMESTAMP',
        'Hora': 'HORA', # Renomeia 'Hora' para 'HORA'
        'DiaDaSemana_PT': 'DIA_SEMANA'
    })

    # Remover acentos apenas de TERÇA e SÁBADO na coluna DIA_SEMANA
    if 'DIA_SEMANA' in df_melted.columns:
        df_melted['DIA_SEMANA'] = df_melted['DIA_SEMANA'].apply(remove_acentos_dia_semana)

    # Adiciona coluna DIA_SEMANA_NUM (0=Segunda, 1=Terça, ..., 6=Domingo)
    if 'DIA_SEMANA' in df_melted.columns:
        df_melted['DIA_SEMANA_NUM'] = df_melted['DIA_SEMANA'].map(DAY_NAME_PT_TO_NUM)

    # Reordenar as colunas para a ordem especificada, incluindo DIA_SEMANA_NUM
    final_melted_cols = [
        'ID_INPUT', 'SUBMERCADO', 'ID_SUBMERCADO', 'TIMESTAMP', 'HORA',
        'DIA_SEMANA_NUM', 'DIA_SEMANA', 'Flag_Feriado', 'Patamar', 'PARAMETRO', 'VALOR'
    ]
    
    # Filtrar apenas as colunas que realmente existem no DataFrame final para evitar erros
    df_melted = df_melted[[col for col in final_melted_cols if col in df_melted.columns]]
    
    # Remover a coluna temporária 'original_param_submercado' se ela ainda existir
    df_melted.drop(columns=['original_param_submercado'], inplace=True, errors='ignore')
    
    print("Operação de 'melt' concluída com sucesso.")
    return df_melted


# --- Loop Principal para cada Mês ---

for month_num in range(1, 13):
    month_str = f"{month_num:02d}"
    year_str = "2026"
    print(f"\n--- Processando Mês: {month_str}/{year_str} ---")

    # 1. Carregar Carga (DataFrame Base)
    carga_file = BASE_DIR / f"resultadosCarga_{year_str}/forecast_carga_{month_str}-{year_str}.csv"
    if not carga_file.exists():
        print(f"❌ Arquivo de Carga não encontrado: {carga_file}. Pulando este mês.")
        continue
    
    carga_df = read_and_clean_data(
        carga_file,
        'csv',
        separator=';',
        cols_to_numeric=['SE', 'S', 'NE', 'N'] 
    )
    carga_df['DataHora'] = pd.to_datetime(carga_df['DataHora'])
    carga_df['Mes'] = carga_df['DataHora'].dt.month
    carga_df['Hora'] = carga_df['DataHora'].dt.hour # Hora 0-23
    carga_df['DiaDaSemana_PT'] = carga_df['DataHora'].dt.day_name().map(DAY_MAP_EN_TO_PT)
    
    # Adiciona Flag_Feriado e Patamar à carga_df com valores de exemplo, se não existirem
    # (No seu exemplo de dados, estas colunas já existem, mas esta parte adiciona resiliência)
    if 'Flag_Feriado' not in carga_df.columns:
        print("⚠️ Coluna 'Flag_Feriado' não encontrada na carga. Criando coluna dummy 'False'.")
        carga_df['Flag_Feriado'] = False
    if 'Patamar' not in carga_df.columns:
        print("⚠️ Coluna 'Patamar' não encontrada na carga. Criando coluna dummy 'N/A'.")
        carga_df['Patamar'] = 'N/A'
    
    # CONVERTE PATAMAR PARA MAIÚSCULAS
    carga_df['Patamar'] = carga_df['Patamar'].str.upper()

    if 'Tipo_Dia_Num' not in carga_df.columns:
        print("⚠️ Coluna 'Tipo_Dia_Num' não encontrada na carga. Criando coluna dummy para merge com PCH/PCT.")
        carga_df['Tipo_Dia_Num'] = 0 # Placeholder, ajuste se houver uma lógica específica

    # 2. Carregar PCH (do CSV individual)
    pch_file = BASE_DIR / f"resultadosPchPct_{year_str}/forecast_PCH_{month_str}-{year_str}.csv"
    if pch_file.exists():
        pch_df = read_and_clean_data(
            pch_file,
            'csv',
            separator=';',
            cols_to_numeric=['PCH - SE', 'PCH - S', 'PCH - NE', 'PCH - N']
        )
        pch_df = pch_df.rename(columns={
            'PCH - SE': 'PCH_SE', 'PCH - S': 'PCH_S', 
            'PCH - NE': 'PCH_NE', 'PCH - N': 'PCH_N'
        })
        
        # Garante que 'Mes' e 'Tipo_Dia_Num' estão no pch_df para o merge
        if 'Mes' not in pch_df.columns: pch_df['Mes'] = carga_df['Mes'].iloc[0] # Assume o mês da carga
        if 'Tipo_Dia_Num' not in pch_df.columns: pch_df['Tipo_Dia_Num'] = 0 # Placeholder
        if 'Hora' not in pch_df.columns: pch_df['Hora'] = pch_df['Hora'].astype(int)
        
        carga_df = pd.merge(carga_df, pch_df[['Mes', 'Tipo_Dia_Num', 'Hora', 'PCH_SE', 'PCH_S', 'PCH_NE', 'PCH_N']], 
                            on=['Mes', 'Tipo_Dia_Num', 'Hora'], how='left', suffixes=('', '_PCH'))
    else:
        print(f"⚠️ Arquivo PCH não encontrado: {pch_file}. PCH data não será incluída.")
        for col in ['PCH_SE', 'PCH_S', 'PCH_NE', 'PCH_N']: carga_df[col] = np.nan

    # 3. Carregar PCT (do CSV individual)
    pct_file = BASE_DIR / f"resultadosPchPct_{year_str}/forecast_PCT_{month_str}-{year_str}.csv"
    if pct_file.exists():
        pct_df = read_and_clean_data(
            pct_file,
            'csv',
            separator=';',
            cols_to_numeric=['PCT - SE', 'PCT - S', 'PCT - NE', 'PCT - N']
        )
        pct_df = pct_df.rename(columns={
            'PCT - SE': 'PCT_SE', 'PCT - S': 'PCT_S', 
            'PCT - NE': 'PCT_NE', 'PCT - N': 'PCT_N'
        })

        # Garante que 'Mes' e 'Tipo_Dia_Num' estão no pct_df para o merge
        if 'Mes' not in pct_df.columns: pct_df['Mes'] = carga_df['Mes'].iloc[0] # Assume o mês da carga
        if 'Tipo_Dia_Num' not in pct_df.columns: pct_df['Tipo_Dia_Num'] = 0 # Placeholder
        if 'Hora' not in pct_df.columns: pct_df['Hora'] = pct_df['Hora'].astype(int)

        carga_df = pd.merge(carga_df, pct_df[['Mes', 'Tipo_Dia_Num', 'Hora', 'PCT_SE', 'PCT_S', 'PCT_NE', 'PCT_N']], 
                            on=['Mes', 'Tipo_Dia_Num', 'Hora'], how='left', suffixes=('', '_PCT'))
    else:
        print(f"⚠️ Arquivo PCT não encontrado: {pct_file}. PCT data não será incluída.")
        for col in ['PCT_SE', 'PCT_S', 'PCT_NE', 'PCT_N']: carga_df[col] = np.nan

    # 4. Carregar Eol/Ufv/Mmgd (do Excel - APENAS EOL/UFV/MMGD)
    eum_file = BASE_DIR / f"resultadosEolUfvMmgd_{year_str}/curtailment_input_{month_str}{year_str}.xlsx"
    if eum_file.exists():
        eum_df = _parse_eum_data(eum_file)
        
        if not eum_df.empty:
            # Colunas de dados de EUM para merge
            eum_merge_data_cols = [col for col in eum_df.columns if col.startswith(('MMGD_', 'UFV_', 'EOL_'))]

            # Merge com base na Hora ajustada do EUM e Dia da Semana da Carga
            carga_df = pd.merge(carga_df, eum_df[['INSTANTE', 'Hora_EUM_Key'] + eum_merge_data_cols], 
                                left_on=['DiaDaSemana_PT', 'Hora'], 
                                right_on=['INSTANTE', 'Hora_EUM_Key'], 
                                how='left', suffixes=('', '_EUM'))
            
            # Remove as colunas auxiliares do merge
            carga_df.drop(columns=['INSTANTE', 'Hora_EUM_Key'], inplace=True, errors='ignore')
            
            # Verifica e avisa sobre NaNs na hora 23 se aplicável
            if (carga_df['Hora'] == 23).any() and carga_df.loc[carga_df['Hora'] == 23, 'MMGD_SE'].isnull().all():
                print(f"⚠️ Aviso: Valores Eol/Ufv/Mmgd para a hora 23:00 (carga_df.Hora=23) são NaN pois não há correspondência nos dados de EUM (HORA 1-23).")
        else:
            print(f"⚠️ Dados de Eol/Ufv/Mmgd do Excel estão vazios ou não foram parseados. Colunas serão NaN.")
            for prefix in ['MMGD', 'UFV', 'EOL']:
                for suffix in ['SE', 'S', 'NE', 'N']: 
                    carga_df[f"{prefix}_{suffix}"] = np.nan
    else:
        print(f"⚠️ Arquivo Eol/Ufv/Mmgd não encontrado: {eum_file}. Eol/Ufv/Mmgd data não será incluída.")
        for prefix in ['MMGD', 'UFV', 'EOL']:
            for suffix in ['SE', 'S', 'NE', 'N']: 
                carga_df[f"{prefix}_{suffix}"] = np.nan

    # 6. Filtrar para os primeiros 7 dias do mês (Ajustado!)
    if 'DataHora' in carga_df.columns:
        # Pega a primeira DataHora real do DataFrame
        first_available_datetime = carga_df['DataHora'].min()
        # Calcula o limite de 7 dias a partir dessa primeira data
        seven_days_later_from_start = first_available_datetime + pd.Timedelta(days=7)
        
        carga_df_filtered = carga_df[(carga_df['DataHora'] >= first_available_datetime) & 
                                     (carga_df['DataHora'] < seven_days_later_from_start)].copy()
        
        if len(carga_df_filtered) != 168:
            print(f"⚠️ Aviso: O DataFrame filtrado para os primeiros 7 dias do mês {month_str} contém {len(carga_df_filtered)} linhas, não as 168 esperadas. Isso indica que o arquivo de carga de entrada não possuía 7 dias completos de dados a partir da primeira DataHora.")
    else:
        print("⚠️ Coluna 'DataHora' não encontrada na carga. Não foi possível filtrar por 7 dias. Usando todo o mês.")
        carga_df_filtered = carga_df.copy()

    # 7. Reordenar colunas para corresponder à sua estrutura de saída, se possível
    # Definindo a ordem das colunas como no seu exemplo
    output_column_order = [
        'Mes', 'Tipo_Dia_Num', 'Hora', 'DataHora', 'Flag_Feriado', 'Patamar', 'SE', 'S', 'NE', 'N',
        'DiaDaSemana_PT',
        'PCH_SE', 'PCH_S', 'PCH_NE', 'PCH_N',
        'PCT_SE', 'PCT_S', 'PCT_NE', 'PCT_N',
        'MMGD_SE', 'MMGD_S', 'MMGD_NE', 'MMGD_N',
        'UFV_SE', 'UFV_S', 'UFV_NE', 'UFV_N',
        'EOL_SE', 'EOL_S', 'EOL_NE', 'EOL_N',
    ]
    
    # Filtrar apenas as colunas que realmente existem no DataFrame final
    final_cols = [col for col in output_column_order if col in carga_df_filtered.columns]
    carga_df_filtered = carga_df_filtered[final_cols]

    # Adiciona coluna DiaDaSemana_Num (0=Segunda, ... 6=Domingo) para exportação de CSVs
    if 'DiaDaSemana_PT' in carga_df_filtered.columns:
        tmp_dia_semana_export = carga_df_filtered['DiaDaSemana_PT'].apply(remove_acentos_dia_semana)
        carga_df_filtered['DiaDaSemana_Num'] = tmp_dia_semana_export.map(DAY_NAME_PT_TO_NUM)

    # 8. Salvar o DataFrame combinado original (não-melted) para CSV
    output_file_name = OUTPUT_DIR / f"combined_input_{month_str}-{year_str}.csv"
    carga_df_to_export = carga_df_filtered.copy()
    if 'DiaDaSemana_PT' in carga_df_to_export.columns:
        carga_df_to_export['DiaDaSemana_PT'] = carga_df_to_export['DiaDaSemana_PT'].apply(remove_acentos_dia_semana)
    # Garante que 'DiaDaSemana_Num' (de 0 a 6) estará presente no CSV salvo
    if 'DiaDaSemana_Num' in carga_df_to_export.columns:
        # Coloca a coluna logo após 'DiaDaSemana_PT', se existir essa coluna
        col_list = list(carga_df_to_export.columns)
        if 'DiaDaSemana_PT' in col_list:
            idx = col_list.index('DiaDaSemana_PT')
            # Remove se já estiver
            col_list.remove('DiaDaSemana_Num')
            col_list.insert(idx + 1, 'DiaDaSemana_Num')
            carga_df_to_export = carga_df_to_export[col_list]
    carga_df_to_export.to_csv(output_file_name, index=False, sep=';', decimal=',')
    print(f"✅ Arquivo combinado salvo para o mês {month_str}/{year_str} em: {output_file_name}")

    # --- NOVA ETAPA: MELT DO DATAFRAME ---
    df_melted = melt_combined_dataframe(carga_df_filtered.copy(), month_num, year_str)

    if not df_melted.empty:
        # 9. Salvar o DataFrame "melted" para CSV
        output_melted_file_name = OUTPUT_DIR / f"melted_input_{month_str}-{year_str}.csv"
        df_melted.to_csv(output_melted_file_name, index=False, sep=';', decimal=',')
        print(f"✅ Arquivo 'melted' salvo para o mês {month_str}/{year_str} em: {output_melted_file_name}")
    else:
        print(f"❌ Nenhum dado 'melted' para salvar para o mês {month_str}/{year_str}.")

print("\n--- Processamento de todos os meses concluído ---")