"""
Parser para extrair valores de PCH e PCT do arquivo SISTEMA.DAT do NEWAVE

Extrai dados da seção "GERACAO DE USINAS NAO SIMULADAS" por subsistema e mês.
Subsistemas: 1=SE, 2=S, 3=NE, 4=N
Tipos: 1=PCH, 2=PCT
"""

import re
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple


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


def parse_pch_pct_sistema_dat(
    arquivo_path: str, 
    ano: int = 2026, 
    usar_ano_seguinte_se_faltar: bool = True,
    valor_padrao_faltante: str = "KNOWN"
) -> Tuple[Dict[str, Dict[int, float]], Dict[str, Dict[int, float]]]:
    """
    Extrai valores de PCH e PCT da seção "GERACAO DE USINAS NAO SIMULADAS".
    
    Args:
        arquivo_path: Caminho do arquivo SISTEMA.DAT
        ano: Ano para extrair os dados
        usar_ano_seguinte_se_faltar: Se True, busca dados do ano seguinte quando faltam meses
        valor_padrao_faltante: Valor a usar quando não há dados disponíveis (padrão: "KNOWN" como string)
    
    Returns:
        Tupla (PCH_dict, PCT_dict) no formato {subsistema: {mes: valor}}
    """
    # Mapeamento de subsistemas
    subsistema_map = {
        1: "SE",  # Sudeste
        2: "S",   # Sul
        3: "NE",  # Nordeste
        4: "N"    # Norte
    }
    
    # Inicializar dicionários de resultados
    pch_dict = {sub: {} for sub in subsistema_map.values()}
    pct_dict = {sub: {} for sub in subsistema_map.values()}
    
    with open(arquivo_path, 'r', encoding='latin-1', errors='ignore') as f:
        linhas = f.readlines()
    
    # Encontrar seção "GERACAO DE USINAS NAO SIMULADAS"
    inicio_secao = False
    i = 0
    
    while i < len(linhas):
        linha = linhas[i].strip()
        
        # Procurar início da seção
        if "GERACAO" in linha.upper() and "USINAS" in linha.upper() and "NAO" in linha.upper():
            inicio_secao = True
            i += 1
            continue
        
        if inicio_secao:
            # Se encontrar "999", fim da seção
            if linha == "999":
                break
            
            # Procurar linha com subsistema e tipo (formato: "   1    1  PCH")
            # Usar linha original (sem strip) para preservar espaços à esquerda
            linha_original = linhas[i]
            match_entrada = re.match(r'^\s+(\d+)\s+(\d+)\s+(\w+)', linha_original)
            if match_entrada:
                subsistema_num = int(match_entrada.group(1))
                tipo_num = int(match_entrada.group(2))
                tipo_nome = match_entrada.group(3).upper()
                
                # Só processar PCH (tipo 1) e PCT (tipo 2)
                if tipo_num not in [1, 2]:
                    i += 1
                    continue
                
                if subsistema_num not in subsistema_map:
                    i += 1
                    continue
                
                subsistema = subsistema_map[subsistema_num]
                i += 1
                
                # Escolher dicionário correto (PCH ou PCT)
                dict_alvo = pch_dict if tipo_num == 1 else pct_dict
                
                # Procurar linha com o ano desejado (ou ano seguinte se faltar dados)
                anos_procurar = [ano]
                if usar_ano_seguinte_se_faltar:
                    anos_procurar.append(ano + 1)
                
                # Processar anos para este tipo e subsistema
                while i < len(linhas):
                    linha_ano_raw = linhas[i]
                    linha_ano = linha_ano_raw.strip()
                    
                    # Verificar PRIMEIRO se é próxima entrada diferente (antes de processar anos)
                    match_prox_entrada = re.match(r'^\s+(\d+)\s+(\d+)\s+\w+', linha_ano_raw)
                    if match_prox_entrada:
                        subsistema_prox = int(match_prox_entrada.group(1))
                        tipo_prox = int(match_prox_entrada.group(2))
                        # Se for diferente subsistema ou tipo, parar ANTES de processar esta linha
                        # Decrementar i para que na próxima iteração do loop externo possamos processar esta entrada
                        if subsistema_prox != subsistema_num or tipo_prox != tipo_num:
                            i -= 1  # Voltar uma linha para processar esta entrada na próxima iteração
                            break
                    
                    # Verificar se é linha de algum dos anos desejados
                    match_ano = None
                    ano_encontrado = None
                    for ano_proc in anos_procurar:
                        # Tentar match sem strip primeiro (pode ter espaços importantes)
                        match_temp = re.match(rf'^\s*{ano_proc}\s+', linha_ano_raw)
                        if not match_temp:
                            match_temp = re.match(rf'^\s*{ano_proc}\s+', linha_ano)
                        if match_temp:
                            match_ano = match_temp
                            ano_encontrado = ano_proc
                            break
                    
                    if match_ano:
                        # Usar linha original (sem strip) para preservar espaços
                        linha_processar = linha_ano_raw
                        # Extrair valores numéricos (incluindo decimais)
                        valores = re.findall(r'(\d+\.?\d*)', linha_processar)
                        
                        if len(valores) >= 1:  # Pelo menos o ano
                            valores_meses = valores[1:]  # Pular o primeiro valor (ano)
                            
                            # Processar valores dos meses
                            valores_float = []
                            for val in valores_meses:
                                try:
                                    valores_float.append(float(val))
                                except ValueError:
                                    valores_float.append(0.0)
                            
                            # Se tiver menos de 12 valores, pode ser que só tenha alguns meses (geralmente os últimos)
                            # Exemplo: linha "2025 ... 1966. 2385." tem apenas nov e dez
                            if len(valores_float) < 12:
                                # Preencher com zeros à esquerda (meses anteriores vazios)
                                valores_completos = [0.0] * 12
                                inicio = 12 - len(valores_float)
                                for j, val in enumerate(valores_float):
                                    valores_completos[inicio + j] = val
                                valores_float = valores_completos
                            elif len(valores_float) >= 12:
                                # Pegar apenas os primeiros 12
                                valores_float = valores_float[:12]
                            
                            # Mapear todos os 12 meses (1=janeiro, 2=fevereiro, ..., 12=dezembro)
                            for mes_destino in range(1, 13):
                                idx = mes_destino - 1  # Índice 0-based
                                if idx < len(valores_float):
                                    valor = valores_float[idx]
                                    if valor > 0:  # Só salvar se for maior que zero
                                        # Priorizar ano principal: se é do ano principal, sempre salvar
                                        # Se é do ano seguinte, só salvar se ainda não tem valor do ano principal
                                        if ano_encontrado == ano:
                                            # Sempre salvar valores do ano principal
                                            dict_alvo[subsistema][mes_destino] = valor
                                        elif ano_encontrado == ano + 1:
                                            # Só salvar se não tiver valor do ano principal para este mês
                                            if mes_destino not in dict_alvo[subsistema] or dict_alvo[subsistema].get(mes_destino, 0) == 0:
                                                dict_alvo[subsistema][mes_destino] = valor
                            
                            # Continuar procurando próximo ano
                            i += 1
                            continue
                    
                    # Se encontrar "999", fim da seção
                    if linha_ano == "999":
                        inicio_secao = False
                        break
                    
                    # Se encontrar "POS", continuar procurando (pode haver mais anos depois)
                    if linha_ano.startswith("POS"):
                        i += 1
                        continue
                    
                    i += 1
                    
                    # Proteção contra loop infinito
                    if i >= len(linhas):
                        break
            
            # Continuar procurando próxima entrada
            i += 1
            continue
        
        i += 1
    
    return pch_dict, pct_dict


def parse_pch_pct_sistema_dat_from_zip(
    zip_path: str, 
    ano: int = 2025, 
    usar_ano_seguinte_se_faltar: bool = True,
    valor_padrao_faltante: str = "KNOWN"
) -> Tuple[Dict[str, Dict[int, float]], Dict[str, Dict[int, float]]]:
    """
    Extrai e parseia PCH e PCT de SISTEMA.DAT de um arquivo ZIP.
    
    Args:
        zip_path: Caminho do arquivo ZIP
        ano: Ano para extrair os dados
        usar_ano_seguinte_se_faltar: Se True, busca dados do ano seguinte quando faltam meses
        valor_padrao_faltante: Valor a usar quando não há dados disponíveis
    
    Returns:
        Tupla (PCH_dict, PCT_dict) no formato {subsistema: {mes: valor}}
    """
    arquivo_temp = extrair_arquivo_do_zip(zip_path, "SISTEMA.DAT")
    
    try:
        resultado = parse_pch_pct_sistema_dat(
            arquivo_temp, 
            ano=ano, 
            usar_ano_seguinte_se_faltar=usar_ano_seguinte_se_faltar,
            valor_padrao_faltante=valor_padrao_faltante
        )
    finally:
        # Limpar arquivo temporário
        Path(arquivo_temp).unlink(missing_ok=True)
    
    return resultado


if __name__ == "__main__":
    # Exemplo de uso
    zip_path = "../deck_newave_2025_11.zip"
    
    print("📂 Extraindo e parseando PCH e PCT de SISTEMA.DAT...")
    pch_dict, pct_dict = parse_pch_pct_sistema_dat_from_zip(zip_path, ano=2025)
    
    print("\n📊 Resultado PCH_dict:")
    print("PCH_dict = {")
    for sub in ["SE", "S", "NE", "N"]:
        valores = ", ".join([f"{mes}: {int(valor)}" for mes, valor in sorted(pch_dict[sub].items())])
        print(f'    "{sub}":   {{{valores}}},')
    print("}")
    
    print("\n📊 Resultado PCT_dict:")
    print("PCT_dict = {")
    for sub in ["SE", "S", "NE", "N"]:
        valores = ", ".join([f"{mes}: {int(valor)}" for mes, valor in sorted(pct_dict[sub].items())])
        print(f'    "{sub}":   {{{valores}}},')
    print("}")
