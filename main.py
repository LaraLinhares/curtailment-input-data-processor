"""
Programa principal para processamento de dados de curtailment.

Este programa agrega o processamento de todas as fontes de dados:
- Carga
- EOL/UFV/MMGD
- PCH/PCT
- Térmica

E fornece métricas detalhadas de desempenho e quantidade de dados.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent))

from src.data_processor import AggregatedProcessor
from src.metrics import ProcessingMetrics


def processar_curtailment(
    base_dir: Path,
    meses: List[int],
    ano: int = 2026,
    output_dir: Optional[Path] = None,
    cenario: str = ""
) -> ProcessingMetrics:
    """
    Função principal de processamento de dados de curtailment.
    
    PIPELINE COMPLETO:
    1. Extração de dados históricos
    2. Criação de curvas típicas
    3. Geração de projeções
    4. Agregação final
    
    Args:
        base_dir: Diretório base com os arquivos de entrada
        meses: Lista de meses a processar (1-12)
        ano: Ano para processamento
        output_dir: Diretório para salvar arquivos processados
        cenario: Nome do cenário (ex: "1_mes", "3_meses")
        
    Returns:
        ProcessingMetrics com todas as métricas coletadas
    """
    # Configurar diretório de saída
    if output_dir is None:
        output_dir = base_dir / "outputs" / f"processamento_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Inicializar métricas globais
    metricas = ProcessingMetrics()
    metricas.cenario = cenario
    metricas.periodo_inicio = f"{min(meses):02d}/{ano}"
    metricas.periodo_fim = f"{max(meses):02d}/{ano}"
    metricas.num_meses_processados = len(meses)
    
    print("\n" + "="*80)
    print("SISTEMA DE PROCESSAMENTO DE DADOS PARA OTIMIZAÇÃO DE CURTAILMENT")
    print("PIPELINE COMPLETO: EXTRAÇÃO → CURVA TÍPICA → PROJEÇÃO → AGREGAÇÃO")
    print("="*80)
    print(f"\n📋 Configuração:")
    print(f"   Cenário: {cenario}")
    print(f"   Período: {metricas.periodo_inicio} a {metricas.periodo_fim}")
    print(f"   Total de meses: {len(meses)}")
    print(f"   Ano: {ano}")
    print(f"   Diretório base: {base_dir}")
    print(f"   Diretório de saída: {output_dir}")
    
    # Criar processador agregado
    processador = AggregatedProcessor(base_dir, ano)
    
    # Executar pipeline completo para todas as fontes
    resultados, periodo_info = processador.processar_pipeline_completo(meses, output_dir)
    
    # Atualizar período com datas reais da carga (se disponível)
    if periodo_info:
        metricas.periodo_inicio = periodo_info['data_inicio'].strftime('%d/%m/%Y')
        metricas.periodo_fim = periodo_info['data_fim'].strftime('%d/%m/%Y')
        print(f"\n📅 Período real processado: {metricas.periodo_inicio} a {metricas.periodo_fim} ({periodo_info['total_dias']} dias)")
    
    # Coletar métricas de todas as fontes
    metricas_fontes = processador.obter_metricas()
    for nome_fonte, metricas_fonte in metricas_fontes.items():
        metricas.metricas_fontes[nome_fonte] = metricas_fonte
    
    # Finalizar métricas
    metricas.finalizar()
    
    # Salvar métricas em JSON
    metricas_path = output_dir / "metricas.json"
    with open(metricas_path, 'w', encoding='utf-8') as f:
        json.dump(metricas.to_dict(), f, indent=2, ensure_ascii=False)
    print(f"\n💾 Métricas salvas em: {metricas_path}")
    
    # Imprimir resumo
    metricas.imprimir_resumo()
    
    return metricas


def main():
    """Função principal para execução standalone."""
    # Configuração padrão
    base_dir = Path(__file__).parent
    ano = 2026
    
    # Exemplo: processar 1 mês
    meses = [1]  # Janeiro
    cenario = "1_mes"
    
    # Processar
    metricas = processar_curtailment(
        base_dir=base_dir,
        meses=meses,
        ano=ano,
        cenario=cenario
    )
    
    return metricas


if __name__ == "__main__":
    main()

