import pandas as pd

# Ler o CSV usando pandas
df = pd.read_csv("termica.csv", sep=';')

# 1. Adicionar coluna CMO com valor 0.0 se não existir
if 'CMO' not in df.columns:
    df['CMO'] = 0.0

# 2. Renomear a coluna Submercado_ID para ID_SUBMERCADO
df = df.rename(columns={'Submercado_ID': 'ID_SUBMERCADO'})

# 3. Identificar as colunas de GMIN por mês
gmin_cols = [col for col in df.columns if col.startswith('GMIN_')]

# 4. Aplicar a regra de despacho forçado: se CVU <= CMO ⇒ GMIN_MÊS = GMAX
# CMO é 0, então a condição será CVU <= 0.0
mask_despacho_forcado = df['CUSTO_CVU'] <= df['CMO']

# Para cada coluna GMIN_MÊS, aplicar a regra
for col in gmin_cols:
    df.loc[mask_despacho_forcado, col] = df.loc[mask_despacho_forcado, 'GMAX']

# Garantir GMIN <= GMAX após o despacho forçado 
for col in gmin_cols:
    over = df[col] > df['GMAX']
    if over.any():
        df.loc[over, col] = df.loc[over, 'GMAX']

# 5. Selecionar e ordenar as colunas para a saída
output_cols = ['ID', 'NOME', 'ID_SUBMERCADO', 'GMAX', 'CMO', 'CUSTO_CVU'] + gmin_cols
df_result = df[output_cols]
df_result.to_csv("termica_despachado.csv", index=False, sep=";")

# Exibir o DataFrame resultante
print("DataFrame com a regra de despacho forçado aplicada:")
print(df_result.head())