import sys
from pathlib import Path

# Adicionar diretório pai ao sys.path para encontrar o módulo utils
sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import os
import pandas as pd
from utils import utils_aws, utils_snowflake

# Configuração do Snowflake
SystemsManager = utils_aws.SSM()
snowflake_credentials = SystemsManager.get_parameter(name='/snowflake/automationservice/rsa',decrypted=False)
Snowflake = utils_snowflake.SnowflakeSession(snowflake_credentials=snowflake_credentials)
Snowflake.connect()


def get_typical_year(month: int, eol_source='EOL_V2', ufv_source='UFV', mmgd_source='GD_AVG'):
    query = f"""
            SELECT
                CASE 
                    WHEN ENERGY_SOURCE = '{eol_source}' THEN 'EOL'
                    WHEN ENERGY_SOURCE = '{ufv_source}' THEN 'UFV'
                    WHEN ENERGY_SOURCE = '{mmgd_source}' THEN 'MMGD'
                    ELSE ENERGY_SOURCE
                END AS ENERGY_SOURCE,
                ID_SUBMARKET,
                METRIC_NAME,
                HOUROFDAY,
                VALUE AS FC
            FROM
                OEM_DEV.ONS.F_REAL_SUBMARKET_CAPACITYFACTOR_HOURLY_MONTHLY
            WHERE
                MONTHOFYEAR = {month}
                AND ENERGY_SOURCE IN ('{eol_source}', '{ufv_source}', '{mmgd_source}')
                AND METRIC_NAME != 'STD'
            ORDER BY ENERGY_SOURCE, ID_SUBMARKET, METRIC_NAME, HOUROFDAY
            """
    response = Snowflake.query_to_dataframe(query)
    return response


def get_installed_capacity(basis : bool = True):
    # basis_condition = "WHERE SCENARIO = 'BASE'"
    basis_condition = "WHERE SCENARIO = 'Dez+'"
    most_recent_cap_condition = "QUALIFY ROW_NUMBER() OVER (PARTITION BY ID_SUBMARKET, ENERGY_SOURCE ORDER BY UPLOADED_AT DESC) = 1"

    query = f"""
            SELECT
                ENERGY_SOURCE,
                ID_SUBMARKET,
                VALUE AS INSTALLED_CAPACITY,
                GENERATION_PERCENTILE
            FROM OEM_DEV.ONS.F_REAL_SUBMARKET_INSTALLEDCAPACITY
            {basis_condition if basis else most_recent_cap_condition}
            ORDER BY ENERGY_SOURCE, ID_SUBMARKET
            """
    response = Snowflake.query_to_dataframe(query)
    return response


class Percentile:
    def __init__(self, source, submarket, percentile, df):
        self.source = source
        self.submarket = submarket
        self.percentile = percentile
        self.available_percentiles = [10, 25, 50, 75, 90]
        self.df = df

    def estimate_percentile(self, known_values):
        """Interpola valores para percentis arbitrários"""
        return np.interp(self.percentile, self.available_percentiles, known_values)

    def process(self):
        query = f"ENERGY_SOURCE == '{self.source}' & ID_SUBMARKET == '{self.submarket}'"
        values_dict = {}
        for p in self.available_percentiles:
            values_dict[p] = self.df.query(f"{query} & METRIC_NAME == 'P{p}'")["POWER"].values

        # interpolar cada hora separadamente
        estimated = []
        for i in range(24):
            known_vals = [values_dict[p][i] for p in self.available_percentiles]
            est = self.estimate_percentile(known_vals)
            estimated.append(est)

        week_values = np.tile(estimated, 7)
        return week_values

if __name__ == "__main__":
    # Parâmetros
    boost = False
    available_percentiles = [10, 25, 50, 75, 90]
    meses = list(range(1, 13))  # Janeiro a Dezembro
    dias_semana = range(7)      # 0 a 6 (domingo a sábado)
    horas = range(24)           # 0 a 23

    # Carregar informações de capacidade instalada uma vez
    installed_capacity = get_installed_capacity()
    cenarios = installed_capacity[["ENERGY_SOURCE", "ID_SUBMARKET", "GENERATION_PERCENTILE"]].copy()
    cenarios.rename({"ENERGY_SOURCE": "FONTE", "ID_SUBMARKET": "SUBMERCADO", "GENERATION_PERCENTILE": "PERCENTIL"}, axis=1, inplace=True)
    cenarios["PERCENTIL"] = [float(x[1:]) if x else 50 for x in cenarios["PERCENTIL"]]
    cenarios = cenarios[cenarios["FONTE"].isin(["EOL", "MMGD", "UFV"])].reset_index(drop=True)

    os.makedirs("resultados_2026", exist_ok=True)

    for month in meses:
        # Prepara estrutura para salvar resultados do mês
        resultados = []
        # Carrega informações de ano típico para o mês selecionado
        typical_year = get_typical_year(month)
        merged = pd.merge(typical_year, installed_capacity, on=["ENERGY_SOURCE", "ID_SUBMARKET"], how="inner")
        merged["VALOR"] = merged["FC"] * merged["INSTALLED_CAPACITY"]

        for _, row in cenarios.iterrows():
            fonte = row['FONTE']
            submercado = row['SUBMERCADO']
            percentil = row['PERCENTIL']

            # Ajuste percentil se boost
            if boost:
                if percentil == 50:
                    continue
                elif percentil < 50:
                    percentil -= 15
                else:
                    percentil += 15
                percentil = np.clip(percentil, 1, 99)

            # Gera vetor de 168 horas (7 dias x 24h) para o cenário/percentil do mês típico
            if (fonte == "UFV") and ((submercado == "S") or (submercado == "N")):
                week_values = [0.0] * 168
            elif (fonte == "EOL") and (submercado == "SE"):
                # Aqui ainda espera 'VALOR' na coluna do DataFrame, então ajuste no Percentile se necessário
                week_values = Percentile(fonte, "S", percentil, merged.rename(columns={"VALOR": "POWER"})).process()
                week_values = [x * 0.004 for x in week_values]
            else:
                if percentil in available_percentiles:
                    hour_values = merged.query(
                        f"ENERGY_SOURCE == '{fonte}' & ID_SUBMARKET == '{submercado}' & METRIC_NAME == 'P{int(percentil)}'"
                    )["VALOR"].values
                    week_values = np.tile(hour_values, 7)
                else:
                    week_values = Percentile(fonte, submercado, percentil, merged.rename(columns={"VALOR": "POWER"})).process()

            # Monta registros para cada hora da semana típica (para cada mês, cenário, hora da semana)
            for week_idx, value in enumerate(week_values):
                dia_semana = week_idx // 24
                hora = week_idx % 24
                resultados.append({
                    "MES": month,
                    "DIA_SEMANA": dia_semana,
                    "HORA": hora,
                    "FONTE": fonte,
                    "SUBMERCADO": submercado,
                    "PERCENTIL": percentil,
                    "VALOR": value
                })

        resultados_df = pd.DataFrame(resultados)
        # Salva um CSV para cada mês
        resultados_df.to_csv(f"resultados_2026/forecast_ufv_mmgd_eol_{month:02d}_2026.csv", sep=";", index=False)
