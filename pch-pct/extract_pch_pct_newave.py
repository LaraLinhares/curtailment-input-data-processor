"""
Script principal para extrair dados de PCH e PCT dos arquivos NEWAVE

Extrai valores da seção "GERACAO DE USINAS NAO SIMULADAS" do SISTEMA.DAT.

Uso:
    python extract_pch_pct_newave.py [ano]
    
    Exemplos:
    python extract_pch_pct_newave.py          # Extrai dados de 2025 (padrão)
    python extract_pch_pct_newave.py 2026     # Extrai dados de 2026
"""

import argparse
import sys
from pathlib import Path

# Adicionar diretório atual ao path
sys.path.insert(0, str(Path(__file__).parent))

from parse_pch_pct_sistema_dat import parse_pch_pct_sistema_dat_from_zip


def formatar_dicionario(d: dict, nome: str) -> str:
    """
    Formata dicionário para exibição Python.
    
    Args:
        d: Dicionário a ser formatado
        nome: Nome da variável
    
    Returns:
        String formatada
    """
    linhas = [f"{nome} = {{"]
    for sub in ["SE", "S", "NE", "N"]:
        # Mostrar todos os meses disponíveis
        meses_disponiveis = sorted([m for m in d[sub].keys() if d[sub][m] > 0])
        
        if meses_disponiveis:
            valores_formatados = []
            for mes in meses_disponiveis:
                valor = int(d[sub][mes])
                valores_formatados.append(f"{mes}: {valor:>5}")
            valores = ", ".join(valores_formatados)
            linhas.append(f'    "{sub}":   {{{valores}}},')
        else:
            linhas.append(f'    "{sub}":   {{}},')
    linhas.append("}")
    return "\n".join(linhas)

def main():
    """
    Função principal que executa a extração e exibe os resultados.
    """
    # Configurar parser de argumentos
    parser = argparse.ArgumentParser(
        description='Extrai dados de PCH e PCT dos arquivos NEWAVE',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  %(prog)s                    # Extrai dados de 2025 (padrão)
  %(prog)s 2026                # Extrai dados de 2026
  %(prog)s 2025 --apenas-ano-solicitado  # Apenas dados de 2025, sem buscar em 2026
        """
    )
    parser.add_argument(
        'ano',
        type=int,
        nargs='?',
        default=2025,
        help='Ano para extrair os dados (padrão: 2025)'
    )
    parser.add_argument(
        '--zip',
        type=str,
        default=None,
        help='Caminho do arquivo ZIP (padrão: deck_newave_2025_11.zip no diretório pai)'
    )
    parser.add_argument(
        '--apenas-ano-solicitado',
        action='store_true',
        help='Mostrar apenas dados do ano solicitado, sem buscar no ano seguinte'
    )
    
    args = parser.parse_args()
    ano = args.ano
    usar_ano_seguinte = not args.apenas_ano_solicitado
    
    # Determinar caminho do ZIP
    if args.zip:
        zip_path = Path(args.zip)
    else:
        zip_path = Path(__file__).parent.parent / "deck_newave_2025_11.zip"
    
    if not zip_path.exists():
        print(f"❌ Erro: Arquivo {zip_path} não encontrado!")
        print(f"   Diretório atual: {Path.cwd()}")
        return
    
    print(f"🔍 Extraindo dados de PCH e PCT do ano {ano}...")
    if usar_ano_seguinte:
        print(f"   ℹ️  Buscando dados de {ano + 1} se faltarem meses em {ano}")
    else:
        print(f"   ℹ️  Mostrando apenas dados disponíveis em {ano}")
    print(f"📦 Arquivo ZIP: {zip_path}\n")
    
    # Extrair dados
    print("=" * 60)
    print("📊 Extraindo PCH e PCT de SISTEMA.DAT")
    print("=" * 60)
    
    pch_dict, pct_dict = parse_pch_pct_sistema_dat_from_zip(
        str(zip_path), 
        ano=ano, 
        usar_ano_seguinte_se_faltar=usar_ano_seguinte,
        valor_padrao_faltante="KNOWN"
    )
    
    # Exibir resultados formatados
    print("\n" + "=" * 60)
    print("📊 RESULTADOS FINAIS - DICIONÁRIOS PYTHON")
    print("=" * 60)
    
    print("\n" + formatar_dicionario(pch_dict, "PCH_dict"))
    print("\n" + formatar_dicionario(pct_dict, "PCT_dict"))
    
    # Verificar quais meses estão disponíveis
    print("\n" + "=" * 60)
    print("✅ Verificação de completude")
    print("=" * 60)
    
    for sub in ["SE", "S", "NE", "N"]:
        meses_pch = sorted([m for m in pch_dict[sub].keys() if pch_dict[sub][m] > 0])
        meses_pct = sorted([m for m in pct_dict[sub].keys() if pct_dict[sub][m] > 0])
        
        print(f"\n{sub}:")
        print(f"  PCH - Meses disponíveis: {meses_pch} ({len(meses_pch)} meses)")
        print(f"  PCT - Meses disponíveis: {meses_pct} ({len(meses_pct)} meses)")
    
    print("\n" + "=" * 60)
    print("💡 Dica: Você pode copiar os dicionários acima para seu código")
    print("=" * 60)


if __name__ == "__main__":
    main()

