import requests
import pandas as pd
import io
from datetime import datetime

def extrair_e_consolidar_carga_ons_csv():
    """
    Baixa os arquivos anuais de curva de carga horária do ONS no formato CSV,
    consolida-os em um único DataFrame e salva o resultado em
    um arquivo CSV local.
    """
    
    # --- 1. CONFIGURAÇÕES ---
    
    ano_inicial = 2015
    # O ano final agora está correto, pois o script anterior já pegava o ano atual.
    ano_final = datetime.now().year 

    # ==================================================================
    # >>>>> INÍCIO DA CORREÇÃO <<<<<
    # URL atualizada com o caminho correto "curva-carga-ho"
    url_base = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/curva-carga-ho/CURVA_CARGA_{}.csv"
    # >>>>> FIM DA CORREÇÃO <<<<<
    # ==================================================================

    lista_dataframes_anuais = []
    
    print(">>> INICIANDO EXTRAÇÃO E CONSOLIDAÇÃO DOS DADOS DO ONS (FORMATO CSV) <<<")
    print(f"Período a ser extraído: {ano_inicial} a {ano_final}\n")

    # --- 2. LOOP DE DOWNLOAD E LEITURA ---
    
    for ano in range(ano_inicial, ano_final + 1):
        url_do_arquivo = url_base.format(ano)
        print(f"Processando ano: {ano}...")
        
        try:
            response = requests.get(url_do_arquivo, timeout=60)
            response.raise_for_status()
            
            arquivo_em_memoria = io.BytesIO(response.content)
            
            # Lê o arquivo CSV, especificando que o separador de colunas é o ponto e vírgula
            df_anual = pd.read_csv(arquivo_em_memoria, delimiter=';', decimal=',')
            
            lista_dataframes_anuais.append(df_anual)
            
            print(f"  -> Sucesso! {len(df_anual)} linhas carregadas para o ano {ano}.")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"  -> Aviso: Arquivo para o ano {ano} não encontrado (URL: {url_do_arquivo}).")
            else:
                print(f"  -> ERRO HTTP: Falha ao baixar o arquivo para o ano {ano}. Motivo: {e}")
        except Exception as e:
            print(f"  -> ERRO: Ocorreu um problema inesperado ao processar o ano {ano}. Motivo: {e}")

    # --- 3. CONSOLIDAÇÃO E SALVAMENTO ---
    
    if not lista_dataframes_anuais:
        print("\nNenhum dado foi baixado. O script será encerrado.")
        return

    print("\n>>> Consolidando todos os dados em uma única tabela...")
    
    df_consolidado = pd.concat(lista_dataframes_anuais, ignore_index=True)
    
    print("-> Garantindo consistência do tipo de dado da coluna 'val_cargaenergiahomwmed'...")
    df_consolidado['val_cargaenergiahomwmed'] = pd.to_numeric(df_consolidado['val_cargaenergiahomwmed'], errors='coerce')

    print("\nTabela consolidada criada com sucesso!")
    print(f"  - Número total de linhas: {len(df_consolidado)}")
    print(f"  - Colunas: {df_consolidado.columns.tolist()}")

    nome_arquivo_csv = "dados_consolidados_curva_carga_ons.csv"
    df_consolidado.to_csv(nome_arquivo_csv, index=False, sep=';', decimal=',', encoding='utf-8-sig')
    
    print(f"\n>>> DADOS SALVOS EM '{nome_arquivo_csv}'")


if __name__ == "__main__":
    extrair_e_consolidar_carga_ons_csv()