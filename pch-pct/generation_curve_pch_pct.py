import os

import numpy as np
import pandas as pd


### Dados de MWmed do PMO NOV/2025 - SISTEMA.DAT
MWmed_dict = {
    "PCH": {
        "SE":   {1:  2735, 2:  2946, 3:  2913, 4:  2630, 5:  2087, 6:  1784, 7:  1533, 8:  1373, 9:  1276, 10:  1572, 11:  1975, 12:  2397},
        "S":   {1:   948, 2:   912, 3:   924, 4:   916, 5:  1066, 6:  1310, 7:  1335, 8:  1088, 9:  1139, 10:  1414, 11:  1145, 12:  1185},
        "NE":   {1:    88, 2:    87, 3:    83, 4:    86, 5:    79, 6:    75, 7:    73, 8:    75, 9:    67, 10:    65, 11:    85, 12:    92},
        "N":   {1:   162, 2:   171, 3:   171, 4:   159, 5:   136, 6:   122, 7:   111, 8:   105, 9:   104, 10:   111, 11:   134, 12:   151}
    },
    "PCT": {
        "SE":   {1:   872, 2:   908, 3:  1262, 4:  2730, 5:  4115, 6:  4276, 7:  4539, 8:  4382, 9:  4166, 10:  3677, 11:  2965, 12:  1488},
        "S":   {1:   377, 2:   361, 3:   394, 4:   456, 5:   531, 6:   528, 7:   532, 8:   527, 9:   543, 10:   511, 11:   509, 12:   370},
        "NE":   {1:   482, 2:   459, 3:   395, 4:   371, 5:   359, 6:   354, 7:   365, 8:   362, 9:   439, 10:   485, 11:   483, 12:   486},
        "N":   {1:   181, 2:   210, 3:   200, 4:   216, 5:   237, 6:   207, 7:   218, 8:   222, 9:   222, 10:   188, 11:   191, 12:   195}
    }
}

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
    nome_arquivo = f"outputs/curva_tipica_{tipo_geracao}_mensal_pua.csv"
    return pd.read_csv(nome_arquivo, sep=';', decimal=',')


def calcular_forecast_subsistema(
    df_anual: pd.DataFrame,
    mes: int,
    subsistema: str,
    tipo_geracao: str
) -> pd.Series:
    """
    Calcula o forecast de geração para um subsistema específico.
    
    Fórmula: (Mean - Std) * MWmed
    
    Args:
        df_anual: DataFrame com dados anuais da curva típica
        mes: Mês a ser processado
        subsistema: Código do subsistema ("SE", "S", "NE", "N")
        tipo_geracao: Tipo de geração ("PCH" ou "PCT")
    
    Returns:
        Series com os valores calculados do forecast
    """
    df_mes = df_anual.loc[df_anual["Mes"] == mes].copy()
    
    col_mean = f"{subsistema}_pu_mean"
    col_std = f"{subsistema}_pu_std"
    
    mean_values = df_mes[col_mean]
    # std_values = df_mes[col_std]
    mwmed = MWmed_dict[tipo_geracao][subsistema][mes]
    
    # Usando somente a média, sem o desvio padrão
    forecast = (mean_values) * mwmed
    
    return forecast


def processar_mes_tipo_geracao(
    mes: int,
    tipo_geracao: str
) -> pd.DataFrame:
    """
    Processa os dados de forecast para um mês e tipo de geração específicos.
    
    Args:
        mes: Mês a ser processado
        tipo_geracao: Tipo de geração ("PCH" ou "PCT")
    
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
            df_anual, mes, subsistema, tipo_geracao
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
    # Criar pasta de saída se não existir
    pasta_resultados = "ResultadosPCH-PCT_2026"
    os.makedirs(pasta_resultados, exist_ok=True)
    
    for mes in MESES:
        for tipo_geracao in TIPOS_GERACAO:
            # Processar dados do mês e tipo de geração
            df_resultado = processar_mes_tipo_geracao(mes, tipo_geracao)
            
            # Definir nome do arquivo de saída (um arquivo por mês e tipo)
            nome_arquivo = os.path.join(
                pasta_resultados,
                f"forecast_{tipo_geracao}_{mes:02d}-2026.csv"
            )
            
            # Salvar CSV
            salvar_csv(df_resultado, nome_arquivo)
            print(f"✓ Arquivo salvo: {nome_arquivo}")


if __name__ == "__main__":
    main()
