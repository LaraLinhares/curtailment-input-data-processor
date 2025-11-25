#!/usr/bin/env python3
"""
Script de exemplo para uso rápido do sistema de processamento.

Este script demonstra diferentes formas de usar o sistema.
"""

import sys
from pathlib import Path

# Adicionar diretório ao path
sys.path.insert(0, str(Path(__file__).parent))

from main import processar_curtailment


def exemplo_1_mes():
    """Exemplo: Processar 1 mês de dados."""
    print("\n" + "="*80)
    print("EXEMPLO 1: PROCESSAMENTO DE 1 MÊS")
    print("="*80)
    
    base_dir = Path(__file__).parent
    meses = [1]  # Janeiro
    
    metricas = processar_curtailment(
        base_dir=base_dir,
        meses=meses,
        ano=2026,
        cenario="exemplo_1_mes"
    )
    
    return metricas


def exemplo_trimestre():
    """Exemplo: Processar um trimestre de dados."""
    print("\n" + "="*80)
    print("EXEMPLO 2: PROCESSAMENTO DE UM TRIMESTRE")
    print("="*80)
    
    base_dir = Path(__file__).parent
    meses = [1, 2, 3]  # Primeiro trimestre
    
    metricas = processar_curtailment(
        base_dir=base_dir,
        meses=meses,
        ano=2026,
        cenario="exemplo_trimestre"
    )
    
    return metricas


def exemplo_semestre():
    """Exemplo: Processar um semestre de dados."""
    print("\n" + "="*80)
    print("EXEMPLO 3: PROCESSAMENTO DE UM SEMESTRE")
    print("="*80)
    
    base_dir = Path(__file__).parent
    meses = [1, 2, 3, 4, 5, 6]  # Primeiro semestre
    
    metricas = processar_curtailment(
        base_dir=base_dir,
        meses=meses,
        ano=2026,
        cenario="exemplo_semestre"
    )
    
    return metricas


def exemplo_ano_completo():
    """Exemplo: Processar ano completo (12 semanas típicas)."""
    print("\n" + "="*80)
    print("EXEMPLO 4: PROCESSAMENTO DE ANO COMPLETO")
    print("="*80)
    
    base_dir = Path(__file__).parent
    meses = list(range(1, 13))  # Todos os 12 meses
    
    metricas = processar_curtailment(
        base_dir=base_dir,
        meses=meses,
        ano=2026,
        cenario="exemplo_ano_completo"
    )
    
    return metricas


def exemplo_meses_especificos():
    """Exemplo: Processar meses específicos (não consecutivos)."""
    print("\n" + "="*80)
    print("EXEMPLO 5: PROCESSAMENTO DE MESES ESPECÍFICOS")
    print("="*80)
    
    base_dir = Path(__file__).parent
    # Processar apenas meses de verão (Dezembro, Janeiro, Fevereiro)
    meses = [1, 2, 12]  
    
    metricas = processar_curtailment(
        base_dir=base_dir,
        meses=meses,
        ano=2026,
        cenario="exemplo_meses_especificos_verao"
    )
    
    return metricas


def menu_interativo():
    """Menu interativo para escolher exemplo."""
    print("\n" + "="*80)
    print("SISTEMA DE PROCESSAMENTO DE DADOS - EXEMPLOS")
    print("="*80)
    print("\nEscolha um exemplo para executar:")
    print("\n1. Processar 1 mês")
    print("2. Processar 1 trimestre (3 meses)")
    print("3. Processar 1 semestre (6 meses)")
    print("4. Processar ano completo (12 meses)")
    print("5. Processar meses específicos (exemplo: verão)")
    print("6. Executar todos os exemplos")
    print("0. Sair")
    
    escolha = input("\nDigite o número da opção desejada: ").strip()
    
    if escolha == "1":
        exemplo_1_mes()
    elif escolha == "2":
        exemplo_trimestre()
    elif escolha == "3":
        exemplo_semestre()
    elif escolha == "4":
        exemplo_ano_completo()
    elif escolha == "5":
        exemplo_meses_especificos()
    elif escolha == "6":
        print("\n🚀 Executando todos os exemplos...")
        exemplo_1_mes()
        exemplo_trimestre()
        exemplo_semestre()
        exemplo_ano_completo()
        exemplo_meses_especificos()
    elif escolha == "0":
        print("\n👋 Até logo!")
        sys.exit(0)
    else:
        print("\n❌ Opção inválida. Tente novamente.")
        menu_interativo()


if __name__ == "__main__":
    # Se executado sem argumentos, mostra menu interativo
    if len(sys.argv) == 1:
        menu_interativo()
    # Se executado com argumento, executa exemplo específico
    elif len(sys.argv) == 2:
        exemplo = sys.argv[1]
        if exemplo == "1":
            exemplo_1_mes()
        elif exemplo == "2":
            exemplo_trimestre()
        elif exemplo == "3":
            exemplo_semestre()
        elif exemplo == "4":
            exemplo_ano_completo()
        elif exemplo == "5":
            exemplo_meses_especificos()
        elif exemplo == "all":
            exemplo_1_mes()
            exemplo_trimestre()
            exemplo_semestre()
            exemplo_ano_completo()
            exemplo_meses_especificos()
        else:
            print(f"❌ Exemplo desconhecido: {exemplo}")
            print("Exemplos disponíveis: 1, 2, 3, 4, 5, all")
            sys.exit(1)
    else:
        print("❌ Uso: python exemplos.py [1|2|3|4|5|all]")
        sys.exit(1)

