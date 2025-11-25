"""
Parser para arquivo C_ADIC.DAT do NEWAVE

Extrai e soma todos os valores de carga adicional por subsistema e mês.
Subsistemas: SUDESTE=SE, SUL=S, NORDESTE=NE, NORTE=N
"""

import re
import zipfile
from pathlib import Path
from typing import Dict, Optional


def extrair_arquivo_do_zip(zip_path: str, arquivo_dentro: str, destino: Optional[str] = None) -> str:
    """
    Extrai um arquivo específico de um ZIP.
    
    Args:
        zip_path: Caminho do arquivo ZIP
        arquivo_dentro: Nome do arquivo dentro do ZIP
        destino: Diretório de destino (opcional, usa diretório temporário)
    
    Returns:
        Caminho do arquivo extraído
    """
    if destino is None:
        destino = Path(zip_path).parent
    
    destino = Path(destino)
    destino.mkdir(parents=True, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extract(arquivo_dentro, destino)
    
    return str(destino / arquivo_dentro)


def parse_cadic_dat(arquivo_path: str, ano: int = 2025, usar_ano_seguinte_se_faltar: bool = True) -> Dict[str, Dict[int, float]]:
    """
    Parseia arquivo C_ADIC.DAT e soma todos os valores por subsistema e mês.
    
    Args:
        arquivo_path: Caminho do arquivo C_ADIC.DAT
        ano: Ano para extrair os dados (padrão: 2025)
    
    Returns:
        Dicionário no formato {subsistema: {mes: soma_valores}}
        Exemplo: {"SE": {6: 3149, 7: 3321, ...}, ...}
    """
    # Mapeamento de nomes de subsistemas
    subsistema_map = {
        "SUDESTE": "SE",
        "SUL": "S",
        "NORDESTE": "NE",
        "NORTE": "N"
    }
    
    # Inicializar dicionário de resultados com zeros para todos os 12 meses
    resultado = {sub: {mes: 0.0 for mes in range(1, 13)} for sub in subsistema_map.values()}
    
    with open(arquivo_path, 'r', encoding='latin-1', errors='ignore') as f:
        linhas = f.readlines()
    
    i = 0
    subsistema_atual = None
    
    while i < len(linhas):
        linha = linhas[i].strip()
        
        # Procurar linha com subsistema (formato: "   1  SUDESTE        CONS.ITAIPU")
        # Padrão: número, nome do subsistema, nome do tipo de carga
        match_subsistema = re.match(r'^\s*(\d+)\s+(SUDESTE|SUL|NORDESTE|NORTE)', linha, re.IGNORECASE)
        
        if match_subsistema:
            nome_subsistema = match_subsistema.group(2).upper()
            if nome_subsistema in subsistema_map:
                subsistema_atual = subsistema_map[nome_subsistema]
                i += 1
                
                # Procurar linha com o ano desejado (ou ano seguinte se faltar dados)
                anos_procurar = [ano]
                if usar_ano_seguinte_se_faltar:
                    anos_procurar.append(ano + 1)
                
                while i < len(linhas):
                    linha_ano = linhas[i].strip()
                    
                    # Verificar se é linha de algum dos anos desejados
                    match_ano = None
                    ano_encontrado = None
                    for ano_proc in anos_procurar:
                        match_temp = re.match(rf'^\s*{ano_proc}\s+', linha_ano)
                        if match_temp:
                            match_ano = match_temp
                            ano_encontrado = ano_proc
                            break
                    
                    if match_ano:
                        # Extrair valores numéricos (incluindo decimais)
                        valores = re.findall(r'(\d+\.?\d*)', linha_ano)
                        
                        if len(valores) >= 1:  # Pelo menos o ano
                            # Pular o primeiro valor (ano)
                            valores_meses = valores[1:]
                            
                            # Para 2025, pode ter apenas 2 valores (nov e dez) ou mais
                            # Se tiver menos de 12 valores, assumir que são os últimos meses
                            if len(valores_meses) < 12:
                                # Preencher com zeros à esquerda
                                valores_completos = [0.0] * 12
                                # Colocar os valores encontrados nas últimas posições
                                inicio = 12 - len(valores_meses)
                                for j, val in enumerate(valores_meses):
                                    try:
                                        valores_completos[inicio + j] = float(val)
                                    except ValueError:
                                        pass
                                valores_meses = valores_completos
                            elif len(valores_meses) >= 12:
                                # Pegar apenas os primeiros 12
                                valores_meses = [float(v) if v else 0.0 for v in valores_meses[:12]]
                            else:
                                valores_meses = []
                            
                            # Mapear todos os 12 meses (1=janeiro, 2=fevereiro, ..., 12=dezembro)
                            # Índices: janeiro=0, fevereiro=1, ..., dezembro=11 (0-based)
                            meses_map = {mes: idx for mes, idx in zip(range(1, 13), range(12))}
                            
                            for mes_destino, idx in meses_map.items():
                                if idx < len(valores_meses):
                                    try:
                                        valor = float(valores_meses[idx])
                                        if valor > 0:  # Só somar se for maior que zero
                                            # Se é do ano principal ou ainda não tem valor, somar
                                            if ano_encontrado == ano or resultado[subsistema_atual][mes_destino] == 0:
                                                resultado[subsistema_atual][mes_destino] += valor
                                    except (ValueError, IndexError, TypeError):
                                        pass
                        
                        # Continuar procurando se ainda faltam meses ou se é ano seguinte
                        i += 1
                        continue
                    
                    # Se encontrar "POS" ou "999" ou próxima entrada, parar
                    if linha_ano.startswith("POS") or linha_ano == "999" or re.match(r'^\s*\d+\s+(SUDESTE|SUL|NORDESTE|NORTE)', linha_ano, re.IGNORECASE):
                        break
                    
                    i += 1
                
                continue
        
        # Se encontrar "999", pode ser fim do arquivo ou próxima seção
        if linha == "999":
            subsistema_atual = None
        
        i += 1
    
    return resultado


def parse_cadic_dat_from_zip(zip_path: str, ano: int = 2025, usar_ano_seguinte_se_faltar: bool = True) -> Dict[str, Dict[int, float]]:
    """
    Extrai e parseia C_ADIC.DAT de um arquivo ZIP.
    
    Args:
        zip_path: Caminho do arquivo ZIP
        ano: Ano para extrair os dados
        usar_ano_seguinte_se_faltar: Se True, busca dados do ano seguinte quando faltam meses
    
    Returns:
        Dicionário no formato {subsistema: {mes: soma_valores}}
    """
    arquivo_temp = extrair_arquivo_do_zip(zip_path, "C_ADIC.DAT")
    
    try:
        resultado = parse_cadic_dat(arquivo_temp, ano, usar_ano_seguinte_se_faltar=usar_ano_seguinte_se_faltar)
    finally:
        # Limpar arquivo temporário
        Path(arquivo_temp).unlink(missing_ok=True)
    
    return resultado


if __name__ == "__main__":
    # Exemplo de uso
    zip_path = "deck_newave_2025_11.zip"
    
    print("📂 Extraindo e parseando C_ADIC.DAT...")
    cadic_dict = parse_cadic_dat_from_zip(zip_path, ano=2025)
    
    print("\n📊 Resultado CAdic_dict:")
    print("CAdic_dict = {")
    for sub in ["SE", "S", "NE", "N"]:
        valores = ", ".join([f"{mes}: {int(valor)}" for mes, valor in sorted(cadic_dict[sub].items())])
        print(f'    "{sub}":   {{{valores}}},')
    print("}")

