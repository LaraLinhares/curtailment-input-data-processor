# Get CCEE files from CKAN API provided by the CCEE and save them in the termica folder

import os
from pathlib import Path

import pandas as pd
import requests

# TODO: Entender como funciona a atualização dos dados que ficam dentro do CKAN e como fazer para atualizar os dados de forma automática.

class GetFiles:
    # Pega o arquivo em https://dadosabertos.ccee.org.br/dataset/custo_variavel_unitario_merchant
    def get_ccee_merchant_files(self):

        package_name = "custo_variavel_unitario_merchant"  
        package_url = f"https://dadosabertos.ccee.org.br/api/3/action/package_show?id={package_name}"
        package_response = requests.get(package_url).json()

        if not package_response.get("success"):
            raise Exception(f"Erro na API CKAN: {package_response.get('error')}")

        resources = package_response["result"]["resources"]
        resource_id = resources[0]["id"]
        print(f"Resource ID encontrado: {resource_id}")

        base_url = "https://dadosabertos.ccee.org.br/api/3/action/datastore_search"
        limit = 2000
        offset = 0
        all_records = []

        while True:
            params = {"resource_id": resource_id, "limit": limit, "offset": offset}
            response = requests.get(base_url, params=params).json()

            if not response.get("success"):
                raise Exception(f"Erro na API CKAN: {response.get('error')}")

            records = response["result"]["records"]
            if not records:
                break

            all_records.extend(records)
            offset += limit
            print(f"Baixados {len(records)} registros, total até agora: {len(all_records)}")

            if len(records) < limit:
                break

        df = pd.DataFrame(all_records)
        return df

    # Pega o arquivo em https://dadosabertos.ccee.org.br/dataset/custo_variavel_unitario_estrutural
    def get_ccee_cvu_files(self):

        package_name = "custo_variavel_unitario_estrutural"  
        package_url = f"https://dadosabertos.ccee.org.br/api/3/action/package_show?id={package_name}"
        package_response = requests.get(package_url).json()

        if not package_response.get("success"):
            raise Exception(f"Erro na API CKAN: {package_response.get('error')}")

        resources = package_response["result"]["resources"]
        resource_id = resources[0]["id"]
        print(f"Resource ID encontrado: {resource_id}")

        base_url = "https://dadosabertos.ccee.org.br/api/3/action/datastore_search"
        limit = 2000
        offset = 0
        all_records = []

        while True:
            params = {"resource_id": resource_id, "limit": limit, "offset": offset}
            response = requests.get(base_url, params=params).json()

            if not response.get("success"):
                raise Exception(f"Erro na API CKAN: {response.get('error')}")

            records = response["result"]["records"]
            if not records:
                break

            all_records.extend(records)
            offset += limit
            print(f"Baixados {len(records)} registros, total até agora: {len(all_records)}")

            if len(records) < limit:
                break

        df = pd.DataFrame(all_records)
        return df

    def get_newave_deck(self, mes: int, ano: int, save_dir: str = None) -> str:
        """
        Baixa o deck NEWAVE (Resultado do Newave) do site da CCEE.
        
        O deck NEWAVE contém todos os arquivos .DAT do modelo de otimização energética do ONS,
        incluindo TERM.DAT (térmicas), SISTEMA.DAT (carga), e o arquivo Excel GTMIN_CCEE.
        
        ONDE ENCONTRAR NA WEB:
        1. Acesse: https://www.ccee.org.br/
        2. Navegue: Menu "Documentos" → "Acervo CCEE"
        3. Busque por: "Resultado do Newave" ou "NW[AAAA][MM]"
        4. Ou acesse diretamente: https://www.ccee.org.br/web/guest/acervo-ccee
        
        ESTRUTURA DO URL:
        - Padrão: https://www.ccee.org.br/documents/80415/[ID]/NW[AAAA][MM].zip/[HASH]
        - Exemplo: https://www.ccee.org.br/documents/80415/30943449/NW202512.zip/1e2944c5-5ba1-2700-1cd6-7561d220188e
        - O arquivo contém GTMIN_CCEE_[MM][AAAA].xlsx dentro do ZIP
        
        OBSERVAÇÕES:
        - Arquivo grande (~3.6 GB para dezembro/2025)
        - Publicado mensalmente pela CCEE (geralmente final do mês anterior)
        - Hash MD5 disponível na página para validação: 9BE50C30B2165072C6277BD78313A0FB (dez/2025)
        
        Args:
            mes: Mês de referência (1-12)
            ano: Ano de referência (AAAA)
            save_dir: Diretório onde salvar o arquivo (padrão: pasta atual)
        
        Returns:
            Caminho completo do arquivo baixado
        
        Raises:
            Exception: Se o download falhar ou o arquivo não for encontrado
        """
        # Formatar mês com 2 dígitos
        mes_str = str(mes).zfill(2)
        
        # Nome do arquivo
        filename = f"NW{ano}{mes_str}.zip"
        
        # URL base da CCEE para downloads de documentos
        # Formato: https://www.ccee.org.br/documents/80415/[ID_DOCUMENTO]/[NOME_ARQUIVO]/[HASH]
        # 
        # IMPORTANTE: O ID do documento e o HASH mudam mensalmente!
        # Para encontrar o URL atualizado:
        # 1. Acesse https://www.ccee.org.br/web/guest/acervo-ccee
        # 2. Busque por "Resultado do Newave - [MM]/[AAAA]"
        # 3. Clique com botão direito no link [ZIP] e copie o endereço
        # 4. Atualize o dicionário NEWAVE_URLS abaixo
        
        # Dicionário com URLs conhecidos (atualize conforme necessário)
        NEWAVE_URLS = {
            "NW202512.zip": "https://www.ccee.org.br/documents/80415/30943449/NW202512.zip/1e2944c5-5ba1-2700-1cd6-7561d220188e",
            # Adicione outros meses/anos conforme necessário:
            # "NW202601.zip": "https://www.ccee.org.br/documents/80415/[ID]/NW202601.zip/[HASH]",
        }
        
        # Verificar se temos a URL para este arquivo
        if filename not in NEWAVE_URLS:
            raise Exception(
                f"URL não configurada para {filename}.\n"
                f"Por favor:\n"
                f"1. Acesse: https://www.ccee.org.br/web/guest/acervo-ccee\n"
                f"2. Busque por: 'Resultado do Newave - {mes_str}/{ano}'\n"
                f"3. Copie a URL do link [ZIP]\n"
                f"4. Adicione ao dicionário NEWAVE_URLS no código\n"
                f"   Exemplo: NEWAVE_URLS['{filename}'] = 'URL_COPIADA'"
            )
        
        url = NEWAVE_URLS[filename]
        
        # Definir diretório de salvamento
        if save_dir is None:
            save_dir = Path(__file__).parent
        else:
            save_dir = Path(save_dir)
        
        save_dir.mkdir(parents=True, exist_ok=True)
        filepath = save_dir / filename
        
        # Verificar se o arquivo já existe
        if filepath.exists():
            print(f"✅ Arquivo já existe: {filepath}")
            print(f"   Tamanho: {filepath.stat().st_size / (1024**2):.2f} MB")
            return str(filepath)
        
        # Baixar o arquivo
        print(f"📥 Baixando deck NEWAVE de {mes_str}/{ano}...")
        print(f"   URL: {url}")
        print(f"   Destino: {filepath}")
        print(f"   ⚠️  ATENÇÃO: Arquivo grande (~3.6 GB), pode demorar alguns minutos...")
        
        try:
            # Fazer requisição com stream=True para arquivos grandes
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Tamanho total do arquivo
            total_size = int(response.headers.get('content-length', 0))
            
            # Baixar em chunks com progresso
            downloaded_size = 0
            chunk_size = 8192  # 8 KB
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Mostrar progresso a cada 10 MB
                        if downloaded_size % (10 * 1024 * 1024) < chunk_size:
                            progress_mb = downloaded_size / (1024**2)
                            total_mb = total_size / (1024**2) if total_size > 0 else 0
                            if total_mb > 0:
                                percent = (downloaded_size / total_size) * 100
                                print(f"   Progresso: {progress_mb:.1f} MB / {total_mb:.1f} MB ({percent:.1f}%)")
                            else:
                                print(f"   Baixados: {progress_mb:.1f} MB")
            
            file_size_mb = filepath.stat().st_size / (1024**2)
            print(f"✅ Download concluído!")
            print(f"   Arquivo salvo: {filepath}")
            print(f"   Tamanho: {file_size_mb:.2f} MB")
            
            return str(filepath)
            
        except requests.exceptions.RequestException as e:
            # Se houver erro, remover arquivo parcial
            if filepath.exists():
                filepath.unlink()
            raise Exception(f"Erro ao baixar o arquivo: {e}")

if __name__ == "__main__":
    get_files = GetFiles()
    data_cvu_merchant = get_files.get_ccee_merchant_files()
    data_cvu_estrutural = get_files.get_ccee_cvu_files()