import os
from pathlib import Path

import numpy as np
import pandas as pd

from parse_pch_pct_sistema_dat import parse_pch_pct_sistema_dat_from_zip

# ============================================================================
# CONFIGURAÇÕES - AJUSTE AQUI OS PARÂMETROS PRINCIPAIS
# ============================================================================

# Ano para extração dos dados
ANO = 2027

# Caminho do arquivo ZIP com os dados NEWAVE
# Use caminho relativo ao diretório pai ou absoluto
ZIP_PATH = "../deck_newave_2025_12.zip"

# Se True, busca dados do ano seguinte quando faltam meses no ano principal
USAR_ANO_SEGUINTE = False

# Nome da pasta onde serão salvos os resultados
# O ano será automaticamente adicionado ao nome
PASTA_RESULTADOS = f"ResultadosPCH-PCT_{ANO}"

# ============================================================================
# CONSTANTES DO SISTEMA
# ============================================================================

SUBSISTEMAS = ["SE", "S", "NE", "N"]
MESES = list(range(1, 13))
TIPOS_GERACAO = ["PCH", "PCT"]


def carregar_curva_tipica(tipo_geracao: str) -> pd.DataFrame:
    """
    Carrega o arquivo CSV da curva típica para o tipo de geração especificado.
    
    Args:
        tipo_geracao: Tipo de geração ("PCH" ou "PCT")
    
    Returns:
        DataFrame com os dados da curva típica
    """
    tipo_geracao = tipo_geracao.lower()
    nome_arquivo = f"pch-pct/outputs/curva_tipica_{tipo_geracao}_mensal_pua.csv"
    
    # Verificar se arquivo existe
    if not Path(nome_arquivo).exists():
        raise FileNotFoundError(
            f"Arquivo de curva típica não encontrado: {nome_arquivo}\n"
            f"Certifique-se de que os arquivos CSV estão na pasta 'outputs/'"
        )
    
    return pd.read_csv(nome_arquivo, sep=';', decimal=',')


def calcular_forecast_subsistema(
    df_anual: pd.DataFrame,
    mes: int,
    subsistema: str,
    tipo_geracao: str,
    mwmed_dict: dict
) -> pd.Series:
    """
    Calcula o forecast de geração para um subsistema específico.
    
    Fórmula: Mean * MWmed
    
    Args:
        df_anual: DataFrame com dados anuais da curva típica
        mes: Mês a ser processado
        subsistema: Código do subsistema ("SE", "S", "NE", "N")
        tipo_geracao: Tipo de geração ("PCH" ou "PCT")
        mwmed_dict: Dicionário com valores de MWmed
    
    Returns:
        Series com os valores calculados do forecast
    """
    df_mes = df_anual.loc[df_anual["Mes"] == mes].copy()
    
    col_mean = f"{subsistema}_pu_mean"
    col_std = f"{subsistema}_pu_std"
    
    mean_values = df_mes[col_mean]
    # std_values = df_mes[col_std]
    mwmed = mwmed_dict[tipo_geracao][subsistema][mes]
    
    # Usando somente a média, sem o desvio padrão
    forecast = mean_values * mwmed
    
    return forecast


def processar_mes_tipo_geracao(
    mes: int,
    tipo_geracao: str,
    mwmed_dict: dict
) -> pd.DataFrame:
    """
    Processa os dados de forecast para um mês e tipo de geração específicos.
    
    Args:
        mes: Mês a ser processado
        tipo_geracao: Tipo de geração ("PCH" ou "PCT")
        mwmed_dict: Dicionário com valores de MWmed
    
    Returns:
        DataFrame com os dados de forecast calculados
    """
    # Carregar dados da curva típica
    df_anual = carregar_curva_tipica(tipo_geracao)
    
    # Criar DataFrame base com colunas de identificação
    df_resultado = df_anual.loc[
        df_anual["Mes"] == mes,
        ["Mes", "Tipo_Dia_Num", "Hora"]
    ].copy()
    
    # Calcular forecast para cada subsistema
    for subsistema in SUBSISTEMAS:
        forecast_values = calcular_forecast_subsistema(
            df_anual, mes, subsistema, tipo_geracao, mwmed_dict
        )
        df_resultado[f"{tipo_geracao} - {subsistema}"] = forecast_values
    
    return df_resultado


def salvar_csv(df: pd.DataFrame, nome_arquivo: str) -> None:
    """
    Salva o DataFrame em um arquivo CSV com vírgula como separador decimal.
    
    Args:
        df: DataFrame a ser salvo
        nome_arquivo: Nome do arquivo de saída
    """
    df.to_csv(nome_arquivo, index=False, sep=';', decimal=',', encoding='utf-8-sig')


def main():
    """
    Função principal que processa os dados de PCH e PCT e gera arquivos CSV separados
    para cada mês e tipo de geração.
    """
    print("=" * 80)
    print(f"🚀 GERAÇÃO DE CURVAS DE PCH E PCT - ANO {ANO}")
    print("=" * 80)
    
    # Verificar se arquivo ZIP existe
    zip_path_completo = Path(__file__).parent / ZIP_PATH
    if not zip_path_completo.exists():
        print(f"❌ Erro: Arquivo ZIP não encontrado: {zip_path_completo}")
        print(f"   Diretório atual: {Path.cwd()}")
        print(f"   Configure o caminho correto na variável ZIP_PATH no topo do arquivo")
        return
    
    # Extrair dados de PCH e PCT do SISTEMA.DAT
    print(f"\n📦 Extraindo dados do arquivo: {zip_path_completo}")
    print(f"📅 Ano de referência: {ANO}")
    print(f"🔄 Usar ano seguinte se faltar dados: {'Sim' if USAR_ANO_SEGUINTE else 'Não'}")
    
    pch_dict, pct_dict = parse_pch_pct_sistema_dat_from_zip(
        zip_path=str(zip_path_completo),
        ano=ANO,
        usar_ano_seguinte_se_faltar=USAR_ANO_SEGUINTE,
        valor_padrao_faltante="KNOWN"
    )
    
    # Criar dicionário MWmed
    mwmed_dict = {
        "PCH": pch_dict,
        "PCT": pct_dict
    }
    
    # Exibir dados extraídos
    print("\n📊 Dados de MWmed extraídos do SISTEMA.DAT:")
    print("-" * 80)
    for tipo in TIPOS_GERACAO:
        print(f"\n{tipo}:")
        for sub in SUBSISTEMAS:
            meses_disponiveis = sorted([m for m in mwmed_dict[tipo][sub].keys()])
            if meses_disponiveis:
                print(f"  {sub}: {len(meses_disponiveis)} meses - {meses_disponiveis}")
            else:
                print(f"  {sub}: Nenhum dado disponível")
    
    # Criar pasta de saída se não existir
    print(f"\n📁 Criando pasta de resultados: {PASTA_RESULTADOS}")
    os.makedirs(PASTA_RESULTADOS, exist_ok=True)
    
    # Processar e gerar arquivos CSV
    print("\n⚙️  Processando curvas de geração...")
    print("-" * 80)
    
    total_arquivos = 0
    for mes in MESES:
        for tipo_geracao in TIPOS_GERACAO:
            try:
                # Processar dados do mês e tipo de geração
                df_resultado = processar_mes_tipo_geracao(mes, tipo_geracao, mwmed_dict)
                
                # Definir nome do arquivo de saída (um arquivo por mês e tipo)
                nome_arquivo = os.path.join(
                    PASTA_RESULTADOS,
                    f"forecast_{tipo_geracao}_{mes:02d}-{ANO}.csv"
                )
                
                # Salvar CSV
                salvar_csv(df_resultado, nome_arquivo)
                print(f"✓ {tipo_geracao} - Mês {mes:02d}: {nome_arquivo}")
                total_arquivos += 1
                
            except KeyError as e:
                print(f"⚠️  {tipo_geracao} - Mês {mes:02d}: Dados não disponíveis no SISTEMA.DAT ({e})")
            except Exception as e:
                print(f"❌ {tipo_geracao} - Mês {mes:02d}: Erro ao processar - {e}")
    
    # Resumo final
    print("\n" + "=" * 80)
    print(f"✅ PROCESSAMENTO CONCLUÍDO")
    print("=" * 80)
    print(f"📊 Total de arquivos gerados: {total_arquivos}")
    print(f"📁 Pasta de saída: {PASTA_RESULTADOS}")
    print("=" * 80)


if __name__ == "__main__":
    main()
