"""
Parser para arquivo SISTEMA.DAT do NEWAVE

Extrai dados de "MERCADO DE ENERGIA TOTAL" por subsistema e mês.
Subsistemas: 1=SE, 2=S, 3=NE, 4=N
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


def parse_sistema_dat(arquivo_path: str, ano: int = 2025, usar_ano_seguinte_se_faltar: bool = True) -> Dict[str, Dict[int, float]]:
    """
    Parseia arquivo SISTEMA.DAT e extrai dados de MERCADO DE ENERGIA TOTAL.
    
    Args:
        arquivo_path: Caminho do arquivo SISTEMA.DAT
        ano: Ano para extrair os dados (padrão: 2025)
    
    Returns:
        Dicionário no formato {subsistema: {mes: valor}}
        Exemplo: {"SE": {6: 39815, 7: 38355, ...}, ...}
    """
    # Mapeamento de subsistemas
    subsistema_map = {
        1: "SE",  # Sudeste
        2: "S",   # Sul
        3: "NE",  # Nordeste
        4: "N"    # Norte
    }
    
    # Inicializar dicionário de resultados vazio (será preenchido conforme encontrar dados)
    resultado = {sub: {} for sub in subsistema_map.values()}
    
    with open(arquivo_path, 'r', encoding='latin-1', errors='ignore') as f:
        linhas = f.readlines()
    
    # Encontrar seção "MERCADO DE ENERGIA TOTAL"
    inicio_secao = False
    inicio_secao_linha = 0
    i = 0
    
    while i < len(linhas):
        linha = linhas[i].strip()
        
        # Procurar início da seção
        if "MERCADO DE ENERGIA TOTAL" in linha.upper():
            inicio_secao = True
            i += 1
            continue
        
        if inicio_secao:
            # Ignorar linhas de cabeçalho (XXX, cabeçalhos, etc)
            if linha.upper() in ['XXX', ''] or 'XXXJAN' in linha.upper() or linha.strip() == '':
                i += 1
                continue
            
            # Se encontrar "999", fim da seção
            if linha == "999":
                break
            
            # Procurar linha com número de subsistema (formato: "   1", "    1", ou apenas "1")
            match_subsistema = re.match(r'^\s*(\d+)\s*$', linha)
            if match_subsistema:
                subsistema_num = int(match_subsistema.group(1))
                
                if subsistema_num in subsistema_map:
                    subsistema = subsistema_map[subsistema_num]
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
                            # Formato pode ter espaços variáveis entre valores
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
                                            if valor > 0:  # Só salvar se for maior que zero
                                                # Se é do ano principal, sempre salvar
                                                # Se é do ano seguinte e ainda não tem valor (ou é zero), salvar
                                                if (ano_encontrado == ano or 
                                                    mes_destino not in resultado[subsistema] or 
                                                    resultado[subsistema].get(mes_destino, 0) == 0):
                                                    resultado[subsistema][mes_destino] = valor
                                        except (ValueError, IndexError, TypeError):
                                            pass
                            
                            # Continuar procurando próximo ano
                            i += 1
                            continue
                        
                        # Se encontrar "POS", fim dos dados deste subsistema - pular até próxima linha
                        if linha_ano.startswith("POS"):
                            i += 1  # Avançar para próxima linha após POS
                            break
                        
                        # Se encontrar "999", fim da seção
                        if linha_ano == "999":
                            inicio_secao = False
                            break
                        
                        # Se encontrar número de subsistema (1-4), é próxima entrada
                        match_prox_subsistema = re.match(r'^\s*(\d+)\s*$', linha_ano)
                        if match_prox_subsistema:
                            num = int(match_prox_subsistema.group(1))
                            if num in subsistema_map and num != subsistema_num:
                                break
                        
                        i += 1
                        
                        # Proteção contra loop infinito
                        if i >= len(linhas):
                            break
                
                # Se encontrou subsistema válido, continuar procurando próximo
                continue
        
        # Se encontrar "999" após a seção, parar
        if inicio_secao and linha == "999":
            break
        
        i += 1
    
    return resultado


def parse_sistema_dat_from_zip(zip_path: str, ano: int = 2025, usar_ano_seguinte_se_faltar: bool = True) -> Dict[str, Dict[int, float]]:
    """
    Extrai e parseia SISTEMA.DAT de um arquivo ZIP.
    
    Args:
        zip_path: Caminho do arquivo ZIP
        ano: Ano para extrair os dados
        usar_ano_seguinte_se_faltar: Se True, busca dados do ano seguinte quando faltam meses
    
    Returns:
        Dicionário no formato {subsistema: {mes: valor}}
    """
    arquivo_temp = extrair_arquivo_do_zip(zip_path, "SISTEMA.DAT")
    
    try:
        resultado = parse_sistema_dat(arquivo_temp, ano, usar_ano_seguinte_se_faltar=usar_ano_seguinte_se_faltar)
    finally:
        # Limpar arquivo temporário
        Path(arquivo_temp).unlink(missing_ok=True)
    
    return resultado


def get_MWmed_dict(zip_path: str, ano: int) -> dict:
    """
    Extrai e retorna o dicionário MWmed (mercado de energia total) para o ano dado a partir de um arquivo ZIP.

    Args:
        zip_path: Caminho do arquivo ZIP contendo SISTEMA.DAT
        ano: Ano para extrair os dados

    Returns:
        Dicionário MWmed_dict no formato {subsistema: {mes: valor}}
    """
    mwmed_dict = parse_sistema_dat_from_zip(zip_path, ano=ano)
    return mwmed_dict

def print_MWmed_dict(mwmed_dict: dict):
    """
    Imprime o dicionário MWmed_dict em formato amigável.

    Args:
        mwmed_dict: Dicionário MWmed retornado por get_MWmed_dict
    """
    print("MWmed = {")
    for sub in ["SE", "S", "NE", "N"]:
        valores = ", ".join([f"{mes}: {int(valor)}" for mes, valor in sorted(mwmed_dict[sub].items())])
        print(f'    "{sub}":   {{{valores}}},')
    print("}")


