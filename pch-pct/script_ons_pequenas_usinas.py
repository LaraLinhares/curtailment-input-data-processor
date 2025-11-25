import requests
import pandas as pd
import io
from datetime import datetime

def extrair_e_consolidar_geracao_usina_2_ho_csv():
    """
    Baixa os arquivos de geração de usinas pequenas do ONS no formato CSV,
    consolida-os em um único DataFrame e salva o resultado em
    um arquivo CSV local.
    
    Formato da URL:
    - Anos anteriores: GERACAO_USINA-2_YYYY.csv (arquivo anual)
    - Ano atual: GERACAO_USINA-2_YYYY_MM.csv (arquivos mensais)
    Exemplos: 
    - GERACAO_USINA-2_2018.csv
    - GERACAO_USINA-2_2025_11.csv
    """
    
    # --- 1. CONFIGURAÇÕES ---
    
    ano_inicial = 2018
    ano_final = datetime.now().year
    mes_final = datetime.now().month  # Mês atual para o ano final

    # URLs base
    url_anual = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{:04d}.csv"
    url_mensal = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{:04d}_{:02d}.csv"
    
    lista_dataframes = []

    print(">>> INICIANDO EXTRAÇÃO E CONSOLIDAÇÃO DOS DADOS DE GERAÇÃO DE USINAS PEQUENAS (FORMATO CSV) <<<")
    print(f"Período a ser extraído: {ano_inicial} a {ano_final}-{mes_final:02d}\n")

    # --- 2. LOOP DE DOWNLOAD E LEITURA ---
    
    total_arquivos_processados = 0
    total_arquivos_nao_encontrados = 0
    
    for ano in range(ano_inicial, ano_final + 1):
        # Define o último mês a processar
        ultimo_mes = 12 if ano < ano_final else mes_final
        
        # Tenta primeiro arquivo anual
        url_do_arquivo_anual = url_anual.format(ano)
        arquivo_anual_encontrado = False
        
        print(f"Processando ano {ano}...", end=" ")
        
        try:
            response = requests.get(url_do_arquivo_anual, timeout=60)
            response.raise_for_status()
            
            arquivo_em_memoria = io.BytesIO(response.content)
            df_anual = pd.read_csv(arquivo_em_memoria, sep=';', decimal=',')
            
            lista_dataframes.append(df_anual)
            total_arquivos_processados += 1
            arquivo_anual_encontrado = True
            
            print(f"✓ Arquivo anual carregado ({len(df_anual)} linhas)")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Se arquivo anual não encontrado, tenta arquivos mensais
                print(f"Arquivo anual não encontrado, tentando arquivos mensais...")
                arquivos_mensais_encontrados = 0
                
                for mes in range(1, ultimo_mes + 1):
                    url_do_arquivo = url_mensal.format(ano, mes)
                    print(f"  {ano}-{mes:02d}...", end=" ")
                    
                    try:
                        response = requests.get(url_do_arquivo, timeout=60)
                        response.raise_for_status()
                        
                        arquivo_em_memoria = io.BytesIO(response.content)
                        df_mensal = pd.read_csv(arquivo_em_memoria, sep=';', decimal=',')
                        
                        lista_dataframes.append(df_mensal)
                        total_arquivos_processados += 1
                        arquivos_mensais_encontrados += 1
                        
                        print(f"✓ {len(df_mensal)} linhas")

                    except requests.exceptions.HTTPError as e2:
                        if e2.response.status_code == 404:
                            print(f"✗ Não encontrado")
                            total_arquivos_nao_encontrados += 1
                        else:
                            print(f"✗ ERRO HTTP {e2.response.status_code}: {e2}")
                    except Exception as e2:
                        print(f"✗ ERRO: {e2}")
                
                if arquivos_mensais_encontrados == 0:
                    print(f"  ⚠️  Nenhum arquivo mensal encontrado para {ano}")
            else:
                print(f"✗ ERRO HTTP {e.response.status_code}: {e}")
                total_arquivos_nao_encontrados += 1
        except Exception as e:
            print(f"✗ ERRO: {e}")
            total_arquivos_nao_encontrados += 1

    # --- 3. CONSOLIDAÇÃO E SALVAMENTO ---
    
    if not lista_dataframes:
        print("\n❌ Nenhum dado foi baixado. O script será encerrado.")
        return

    print(f"\n>>> Consolidando todos os dados em uma única tabela...")
    print(f"  - Arquivos processados com sucesso: {total_arquivos_processados}")
    print(f"  - Arquivos não encontrados: {total_arquivos_nao_encontrados}")
    
    df_consolidado = pd.concat(lista_dataframes, ignore_index=True)
    
    print("\n-> Garantindo consistência dos tipos de dados...")
    
    # Converte coluna de data se existir
    if 'din_instante' in df_consolidado.columns:
        df_consolidado['din_instante'] = pd.to_datetime(df_consolidado['din_instante'], errors='coerce')
    
    # Converte coluna de geração para numérico
    if 'val_geracao' in df_consolidado.columns:
        df_consolidado['val_geracao'] = pd.to_numeric(df_consolidado['val_geracao'], errors='coerce')

    print("\n✅ Tabela consolidada criada com sucesso!")
    print(f"  - Número total de linhas: {len(df_consolidado):,}")
    print(f"  - Número de colunas: {len(df_consolidado.columns)}")
    print(f"  - Colunas: {df_consolidado.columns.tolist()}")

    nome_arquivo_csv = "dados_consolidados_geracao_usina_2_ho.csv"
    df_consolidado.to_csv(nome_arquivo_csv, index=False, sep=';', decimal=',', encoding='utf-8-sig')
    
    print(f"\n>>> DADOS SALVOS EM '{nome_arquivo_csv}'")


if __name__ == "__main__":
    extrair_e_consolidar_geracao_usina_2_ho_csv()