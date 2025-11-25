# Get CCEE files from CKAN API provided by the CCEE and save them in the termica folder

import requests
import pandas as pd

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

if __name__ == "__main__":
    get_files = GetFiles()
    data_cvu_merchant = get_files.get_ccee_merchant_files()
    data_cvu_estrutural = get_files.get_ccee_cvu_files()