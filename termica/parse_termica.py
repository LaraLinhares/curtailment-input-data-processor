import pandas as pd
import zipfile
import re
import os
from pathlib import Path
import locale

from get_files import GetFiles

# Tenta configurar o locale para Português do Brasil para garantir consistência
# Isso é uma boa prática, mas para strftime('%b') o mapeamento explícito é mais robusto
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR')
    except locale.Error:
        # print("Aviso: Não foi possível definir o locale 'pt_BR'. Algumas conversões de data podem não funcionar como esperado.")
        pass # Ignora se não conseguir definir, pois usaremos mapeamento explícito para meses

def _parse_conft_dat(zfile: zipfile.ZipFile) -> pd.DataFrame:
    """
    Função auxiliar para ler o CONFT.DAT a partir de um objeto ZipFile aberto.
    Retorna um DataFrame com 'ID' e 'SSIS'.
    """
    all_files_in_zip = zfile.namelist()
    
    conft_dat_files = [f for f in all_files_in_zip if os.path.basename(f).upper() == "CONFT.DAT"]
    
    if not conft_dat_files:
        print("❌ Nenhum arquivo 'CONFT.DAT' (case-insensitive, apenas nome base) encontrado no ZIP.")
        return pd.DataFrame(columns=['ID', 'SSIS'])

    conft_file_name = conft_dat_files[0]
    print(f"✅ 'CONFT.DAT' encontrado como: '{conft_file_name}' no ZIP: '{zfile.filename}'")
    
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
    """
    Função auxiliar para ler o CLAST.DAT a partir de um objeto ZipFile aberto.
    Retorna um DataFrame com 'ID', 'TIPO_COMBUSTIVEL', 'CUSTO_CVU'.
    A coluna 'CUSTO_CVU' é extraída da primeira coluna de custo.
    """
    all_files_in_zip = zfile.namelist()

    clast_dat_files = [f for f in all_files_in_zip if os.path.basename(f).upper() == "CLAST.DAT"]

    if not clast_dat_files:
        print("❌ Nenhum arquivo 'CLAST.DAT' (case-insensitive, apenas nome base) encontrado no ZIP.")
        return pd.DataFrame(columns=['ID', 'TIPO_COMBUSTIVEL', 'CUSTO_CVU'])

    clast_file_name = clast_dat_files[0]
    print(f"✅ 'CLAST.DAT' encontrado como: '{clast_file_name}' no ZIP: '{zfile.filename}'")
    
    lines = zfile.open(clast_file_name).read().decode("latin1").splitlines()

    clast_registros = []

    # Regex melhorado para capturar ID, Nome, Tipo de Combustível e todos os custos
    clast_regex = re.compile(r"^\s*(\d+)\s+(.+?)\s+(.+?)\s+([\d\.]+(?:\s+[\d\.]+)*)\s*$")

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
            # Nome da usina é o grupo 2 (não estamos usando aqui)
            tipo_combustivel = match.group(3).strip()
            # Extrai todos os custos e pega o primeiro
            custos_str = match.group(4).split()
            if custos_str:
                custo_cvu = float(custos_str[0].replace(",", "."))
            else:
                custo_cvu = 0.0

            clast_registros.append({
                "ID": usina_id,
                "TIPO_COMBUSTIVEL": tipo_combustivel,
                "CUSTO_CVU": custo_cvu
            })
        except (ValueError, IndexError) as e:
            print(f"⚠️ Erro de conversão para int/float na linha {i+1} do CLAST.DAT: '{line.strip()}' - {e}")
            continue

    return pd.DataFrame(clast_registros)

def _parse_cvu_files(df: pd.DataFrame) -> pd.DataFrame:
    """
    Função auxiliar para ler os arquivos de CVU Merchant e CVU Estrutural a partir de um dataframe com os dados.
    Retorna um DataFrame com 'ID', 'CUSTO_CVU_MERCHANT', 'CUSTO_CVU_ESTRUTURAL'.
    """
    df['CUSTO_CVU_MERCHANT'] = df['CUSTO_CVU_MERCHANT'].astype(float)
    df['CUSTO_CVU_ESTRUTURAL'] = df['CUSTO_CVU_ESTRUTURAL'].astype(float)
    return df


def _parse_gtmin_excel(zfile: zipfile.ZipFile, excel_file_name_in_zip: str) -> pd.DataFrame:
    """
    Função auxiliar para ler o arquivo Excel a partir de um objeto ZipFile.
    Calcula o GMIN para 2026 como o máximo entre 'Gtmin_Agente' e 'Gtmin_Eletrico'.
    Retorna um DataFrame com 'ID', 'MES_ABBR', 'GMIN'.
    """
    print(f"\n--- Investigando '{excel_file_name_in_zip}' dentro do ZIP: '{zfile.filename}' ---")
    
    if excel_file_name_in_zip not in zfile.namelist():
        print(f"❌ Arquivo Excel '{excel_file_name_in_zip}' não encontrado dentro do ZIP: '{zfile.filename}'.")
        return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])

    try:
        with zfile.open(excel_file_name_in_zip) as excel_file:
            df_excel = pd.read_excel(excel_file)
        print(f"✅ Arquivo Excel '{excel_file_name_in_zip}' lido com sucesso.")
        print(f"Colunas do Excel: {df_excel.columns.tolist()}")
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo Excel '{excel_file_name_in_zip}' do ZIP: {e}")
        return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])

    df_excel.rename(columns={
        'código': 'ID',
        'nome': 'NOME_EXCEL', 
        'mês': 'MES',
        'Gtmin_Agente': 'GTMIN_AGENTE',
        'Gtmin_Eletrico': 'GTMIN_ELETRICO'
    }, inplace=True)

    initial_invalid_dates = False
    try:
        df_excel['MES_DT'] = pd.to_datetime(df_excel['MES'], format='%b/%y', errors='coerce')
        if df_excel['MES_DT'].isnull().any():
            print("⚠️ Algumas datas na coluna 'mês' não puderam ser parseadas com '%b/%y'. Tentando inferir...")
            df_excel['MES_DT'] = pd.to_datetime(df_excel['MES'], errors='coerce')
            if df_excel['MES_DT'].isnull().any():
                initial_invalid_dates = True
    except Exception as e:
        print(f"❌ Erro crítico ao converter coluna 'mês' para datetime: {e}. Verifique o formato do Excel.")
        return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])

    if initial_invalid_dates:
        print("❌ Ainda há datas inválidas na coluna 'mês' após tentativas de inferência.")
        problematic_mes_values = df_excel.loc[df_excel['MES_DT'].isnull(), 'MES'].unique()
        print(f"Valores problemáticos encontrados na coluna 'mês': {problematic_mes_values}")
        print("As linhas contendo esses valores serão descartadas para o cálculo do GMIN de 2026.")
        
        df_excel.dropna(subset=['MES_DT'], inplace=True)
        if df_excel.empty:
            print("Após descartar linhas com datas inválidas, o DataFrame do Excel ficou vazio.")
            return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])

    df_2026 = df_excel[df_excel['MES_DT'].dt.year == 2026].copy()

    if df_2026.empty:
        print("⚠️ Nenhuma linha encontrada para o ano de 2026 no arquivo Excel (após filtragem de datas).")
        return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])
    
    for col in ['GTMIN_AGENTE', 'GTMIN_ELETRICO']:
        if col in df_2026.columns:
            df_2026[col] = df_2026[col].astype(str).str.replace(',', '.', regex=False)
            # Correção do FutureWarning: Não usar inplace=True em slices
            df_2026[col] = pd.to_numeric(df_2026[col], errors='coerce').fillna(0)
            # df_2026[col].fillna(0, inplace=True) # REMOVIDO
            
    df_2026['GMIN_NEW'] = df_2026[['GTMIN_AGENTE', 'GTMIN_ELETRICO']].max(axis=1)
    
    # Mapeamento explícito para abreviações de meses em português (para evitar problemas de locale)
    month_abbr_map = {
        1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
        7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
    }
    df_2026['MES_ABBR'] = df_2026['MES_DT'].dt.month.map(month_abbr_map)

    print(f"Linhas para 2026 processadas. Exemplo de GMIN_NEW calculado:")
    print(df_2026[['ID', 'MES', 'GTMIN_AGENTE', 'GTMIN_ELETRICO', 'GMIN_NEW', 'MES_ABBR']].head())
    print("--- Fim da Investigação de GTMIN_CCEE_112025.xlsx ---\n")

    return df_2026[['ID', 'MES_ABBR', 'GMIN_NEW']]

def _get_cvu_col(df: pd.DataFrame, codigo: int, col: str, prefer_mes_col: str = 'MES_REFERENCIA', prefer_ano_col: str = 'ANO_HORIZONTE', silent: bool = True) -> float | None:
    """
    Função auxiliar para obter o valor de uma coluna de um dataframe de CVU a partir de um código de modelo de preço.
    Filtra as linhas desse código, pega a de maior MES_REFERENCIA, retorna col (converte para float, se possível, lida com '-').
    Retorna o valor da coluna ou None se não foi possível encontrar o valor.
    
    Args:
        df: DataFrame com dados de CVU
        codigo: Código do modelo de preço (corresponde ao ID da usina)
        col: Nome da coluna a extrair
        prefer_mes_col: Coluna de referência temporal para pegar o mais recente
        silent: Se True, não imprime mensagens de erro quando não encontrar (padrão: True)
    """
    df_code = df[df['CODIGO_MODELO_PRECO'] == codigo]
    if df_code.empty:
        if not silent:
            print(f"❌ Não foi possível encontrar o código {codigo} no dataframe de CVU {col}.")
        return None
    # pega linha mais recente
    idx = df_code[prefer_mes_col].astype(str).idxmax()
    row = df_code.loc[idx]
    value = row.get(col)
    if value is None or (isinstance(value, float) and pd.isna(value)):
        if not silent:
            print(f"❌ Não foi possível encontrar o valor {value} na coluna {col} para o código {codigo}.")
        return None
    # tratar string '-'
    if isinstance(value, str):
        value = value.replace(',', '.')
        if value.strip() == '-':
            if not silent:
                print(f"❌ Não foi possível encontrar o valor {value} na coluna {col} para o código {codigo}.")
            return None
    try:
        value = float(value)
    except Exception:
        if not silent:
            print(f"❌ Não foi possível converter o valor {value} para float na coluna {col} para o código {codigo}.")
        return None
    # Se valor igual a 0, considera como válido (algumas usinas podem ter cvu 0)
    return value

def _get_cvu_from_ccee(
    df_term: pd.DataFrame,
    data_cvu_merchant: pd.DataFrame,
    data_cvu_estrutural: pd.DataFrame
) -> pd.DataFrame:
    """
    Função auxiliar para obter os valores de CVU a partir de múltiplas fontes.

    Lógica de prioridade correta:
    1. Para cada usina (ID):
        a. Tenta obter CVU_ESTRUTURAL da CCEE (usando ID como CODIGO_MODELO_PRECO)
        b. Se não encontrar (None ou NaN), tenta obter CVU_CF do Merchant
        c. Se não encontrar no CF (None ou NaN), tenta obter CVU_SCF do Merchant
        d. Se não houver em nenhuma fonte acima, atribui None e fonte 'NENHUMA_FONTE'
    Retorna DataFrame com 'ID', 'CUSTO_CVU', 'FONTE'.
    """
    res = []

    for _, row in df_term.iterrows():
        usina_id = row['ID']
        codigo = usina_id

        # Inicializa cvu e fonte como None/CLAST
        cvu_final = None
        fonte = "NENHUMA_FONTE"

        # 1. Tenta CVU_ESTRUTURAL (CCEE estrutural) para ANO_HORIZONTE == 2026
        data_cvu_estrutural_2026 = data_cvu_estrutural[data_cvu_estrutural["ANO_HORIZONTE"] == 2026]
        cvu_estr = _get_cvu_col(data_cvu_estrutural_2026, codigo, 'CVU_ESTRUTURAL', 'MES_REFERENCIA', 'ANO_HORIZONTE')
        if cvu_estr is not None and not pd.isna(cvu_estr):
            cvu_final = cvu_estr
            fonte = 'CVU_ESTRUTURAL'
        else:
            # 2. Tenta CVU_CF (Merchant)
            cvu_cf = _get_cvu_col(data_cvu_merchant, codigo, 'CVU_CF', 'MES_REFERENCIA', 'ANO_HORIZONTE')
            if cvu_cf is not None and not pd.isna(cvu_cf):
                cvu_final = cvu_cf
                fonte = 'CVU_MERCHANT_CF'
            else:
                # 3. Tenta CVU_SCF (Merchant)
                cvu_scf = _get_cvu_col(data_cvu_merchant, codigo, 'CVU_SCF', 'MES_REFERENCIA')
                if cvu_scf is not None and not pd.isna(cvu_scf):
                    cvu_final = cvu_scf
                    fonte = 'CVU_MERCHANT_SCF'
                # 4. Se não encontrou em nenhuma das fontes, mantém valor None e fonte 'NENHUMA_FONTE', atualiza term_df com GMIN=GMAX=0
                if cvu_final is None:
                    cvu_final = 0.0
                    fonte = 'NENHUMA_FONTE'
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_JAN'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_FEV'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_MAR'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_ABR'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_MAI'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_JUN'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_JUL'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_AGO'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_SET'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_OUT'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_NOV'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMIN_DEZ'] = 0.0
                    df_term.loc[df_term['ID'] == usina_id, 'GMAX'] = 0.0

        res.append({'ID': usina_id, 'CUSTO_CVU': cvu_final, 'FONTE': fonte})

    df_result = pd.DataFrame(res)
    df_result.to_csv("teste_cvu_ccee.csv", index=False, sep=";")

    # Relatório de fontes
    print("\n=== Relatório de Fontes de CVU ===")
    fonte_counts = df_result['FONTE'].value_counts(dropna=False)
    for fonte, count in fonte_counts.items():
        print(f"  {fonte}: {count} usinas")
    print(f"  TOTAL: {len(df_result)} usinas")
    print("=" * 35 + "\n")

    return df_result

def parse_term_dat(zip_path: str, excel_file_name_in_zip: str, data_cvu_merchant: pd.DataFrame, data_cvu_estrutural: pd.DataFrame) -> pd.DataFrame:
    """
    Lê TERM.DAT, CONFT.DAT, CLAST.DAT e um Excel de GMIN, todos de dentro do ZIP.
    Atualiza os valores de GMIN para o ano de 2026 com base no arquivo Excel.
    """
    with zipfile.ZipFile(zip_path, "r") as z:
        # --- 1. Parsing TERM.DAT ---
        term_dat_files = [f for f in z.namelist() if os.path.basename(f).upper() == "TERM.DAT"]
        if not term_dat_files:
            raise FileNotFoundError(f"Nenhum arquivo 'TERM.DAT' encontrado no ZIP: '{zip_path}'.")
        term_file_name = term_dat_files[0]
        term_lines = z.open(term_file_name).read().decode("latin1").splitlines()

        registros_term = []
        # Correção do regex para TERM.DAT: usar '\s{2,}' para separar NOME e GMAX
        # Isso garante que NOME como "ANGRA 1" seja capturado corretamente.
        regex_term = re.compile(r"^\s*(\d+)\s+(.+?)\s{2,}(\d+\.?\d*)\s+(\d+\.?\d*)\s+([\d\.]+)\s+([\d\.]+)\s+(.*)$")
        
        meses = ["JAN","FEV","MAR","ABR","MAI","JUN","JUL","AGO","SET","OUT","NOV","DEZ"]

        print(f"Lendo o arquivo TERM.DAT: {term_file_name} do ZIP: '{zip_path}'")
        if len(term_lines) > 2:
            print(f"Primeira linha de dados (TERM.DAT): '{term_lines[2].strip()}'")

        for i, line in enumerate(term_lines):
            if i < 2 or not line.strip(): 
                continue

            match = regex_term.match(line)
            if not match:
                print(f"⚠️ Linha {i+1} do TERM.DAT não corresponde ao padrão esperado: '{line.strip()}' (Regex: {regex_term.pattern})")
                continue

            try:
                usina_id = int(match.group(1))
                nome = match.group(2).strip()
                pot = float(match.group(3)) # GMAX agora deve ser capturado corretamente

                valores_str = match.group(7).split() 
                gmin_term_values = []
                for v_str in valores_str[:12]: 
                    try:
                        gmin_term_values.append(float(v_str.replace(",", ".")))
                    except ValueError:
                        print(f"⚠️ Erro ao converter valor '{v_str}' para float na linha {i+1} do TERM.DAT: '{line.strip()}' (GMINs originais). Usando 0.0.")
                        gmin_term_values.append(0.0) 
                
                while len(gmin_term_values) < 12:
                    gmin_term_values.append(0.0)

                registro = {
                    "ID": usina_id,
                    "NOME": nome,
                    "GMAX": pot,
                }

                for mes_abbr, valor in zip(meses, gmin_term_values):
                    registro[f"GMIN_{mes_abbr}"] = valor

                registros_term.append(registro)
            except ValueError as e:
                print(f"⚠️ Erro de conversão de tipo na linha {i+1} do TERM.DAT: '{line.strip()}' - {e}")
                continue

        df_term = pd.DataFrame(registros_term)
        
        # --- 2. Parsing CONFT.DAT ---
        df_conft = _parse_conft_dat(z)

        # --- 3. Parsing GTMIN_CCEE_112025.xlsx de dentro do ZIP ---
        df_gtmin_excel = _parse_gtmin_excel(z, excel_file_name_in_zip)

        # --- 4. Parsing CLAST.DAT ---
        df_clast = _parse_clast_dat(z)

        # --- 5. Getting CVU from CCEE (com valores do CLAST como base) ---
        df_cvu_ccee = _get_cvu_from_ccee(df_term, data_cvu_merchant, data_cvu_estrutural)

        # --- 6. Consolidar valores de GMIN para 2026 ---
        if not df_gtmin_excel.empty:
            print("\nConsolidando valores de GMIN do Excel para o ano de 2026...")
            
            df_term_ids = set(df_term['ID'].unique())
            
            for _, row in df_gtmin_excel.iterrows():
                usina_id = row['ID']
                mes_abbr = row['MES_ABBR'] # Agora usa abreviações em português
                gmin_new_value = row['GMIN_NEW']

                if usina_id in df_term_ids:
                    col_name = f"GMIN_{mes_abbr}"
                    df_term.loc[df_term['ID'] == usina_id, col_name] = gmin_new_value
            print("GMINs para 2026 atualizados a partir do Excel (apenas para usinas com POT no TERM.DAT).")
        else:
            print("Aviso: O arquivo Excel GTMIN não pôde ser parseado ou está vazio. GMINs não foram atualizados para 2026.")

        # --- 7. Merge dos DataFrames (Term, Conft, CVU, Clast) ---
        df_final = df_term.copy()

        if not df_conft.empty:
            df_final = df_final.merge(df_conft, on='ID', how='left')
        else:
            print("Aviso: CONFT.DAT não pôde ser parseado ou está vazio. A coluna 'SSIS' não foi adicionada.")

        # Merge com CVU (que já inclui dados do CLAST + CCEE)
        if not df_cvu_ccee.empty:
            df_final = df_final.merge(df_cvu_ccee[['ID', 'CUSTO_CVU', 'FONTE']], on='ID', how='left')
        else:
            print("Aviso: CVU não pôde ser obtido. As colunas 'CUSTO_CVU' e 'FONTE' não foram adicionadas.")
        
        # Merge com informações adicionais do CLAST (tipo de combustível)
        if not df_clast.empty and 'TIPO_COMBUSTIVEL' in df_clast.columns:
            df_final = df_final.merge(df_clast[['ID', 'TIPO_COMBUSTIVEL']], on='ID', how='left')
        else:
            print("Aviso: CLAST.DAT não pôde ser parseado ou está vazio. A coluna 'TIPO_COMBUSTIVEL' não foi adicionada.")

        return df_final


def __main__():
    # --- 1. Get CVU Merchant and CVU Estrutural from CCEE ---
    get_files = GetFiles()
    data_cvu_merchant = get_files.get_ccee_merchant_files()
    data_cvu_estrutural = get_files.get_ccee_cvu_files()

    # --- 2. Parse TERM.DAT, CONFT.DAT, CLAST.DAT and GTMIN_CCEE_112025.xlsx from NEWAVE ---
    zip_path = Path(__file__).parent / "NW202511.zip"
    excel_file_name_in_zip = "GTMIN_CCEE_112025.xlsx" # Nome do arquivo Excel DENTRO do ZIP
    
    if not zip_path.exists():
        print(f"Erro: Arquivo ZIP não encontrado no caminho esperado: {zip_path}")
        return

    df = parse_term_dat(str(zip_path), excel_file_name_in_zip, data_cvu_merchant, data_cvu_estrutural) 
    print("\nDataFrame final (com SSIS, CLAST e GMINs de 2026 atualizados do Excel):")
    print(df.head())
    print(f"\nColunas do DataFrame final: {df.columns.tolist()}")

    # --- Aplicação da regra de despacho forçado com CUSTO_CVU (mantida para a saída final) ---
    df['CMO'] = 0.0 # Define CMO
    df = df.rename(columns={'SSIS': 'Submercado_ID'}) # Renomeia SSIS para Submercado_ID
    gmin_cols = [col for col in df.columns if col.startswith('GMIN_')]

    if 'CUSTO_CVU' in df.columns:
        mask_despacho_forcado = df['CUSTO_CVU'] <= df['CMO']

        for col in gmin_cols:
            # Correção do FutureWarning: Não usar inplace=True em slices
            df.loc[mask_despacho_forcado, col] = df.loc[mask_despacho_forcado, 'GMAX']

        # Garantir GMIN <= GMAX após o despacho forçado
        for col in gmin_cols:
            # Correção do FutureWarning: Não usar inplace=True em slices
            over = df[col] > df['GMAX']
            if over.any():
                df.loc[over, col] = df.loc[over, 'GMAX']
    else:
        print("Aviso: 'CUSTO_CVU' não encontrado no DataFrame. A regra de despacho forçado não pôde ser aplicada.")

    # Exibe o DataFrame com a regra aplicada
    print("\nDataFrame com a regra de despacho forçado aplicada:")
    output_cols_final = ['ID', 'NOME', 'Submercado_ID', 'GMAX', 'CMO', 'CUSTO_CVU'] + gmin_cols + ['TIPO_COMBUSTIVEL']
    output_cols_final = [col for col in output_cols_final if col in df.columns]
    print(df[output_cols_final].head())

    df.to_csv("termica.csv", index=False, sep=";")
    print("\nArquivo 'termica.csv' gerado com as colunas mescladas e regra de despacho aplicada.")


if __name__ == "__main__":
    __main__()