"""
Script principal para extrair dados de MWmed e CAdic dos arquivos NEWAVE

Combina dados de SISTEMA.DAT e C_ADIC.DAT para gerar os dicionários finais.

Uso:
    python extract_newave_data.py [ano]
    
    Exemplos:
    python extract_newave_data.py          # Extrai dados de 2025 (padrão)
    python extract_newave_data.py 2026     # Extrai dados de 2026
    python extract_newave_data.py 2024     # Extrai dados de 2024
"""

import argparse
import sys
from pathlib import Path

# Adicionar diretório atual ao path
sys.path.insert(0, str(Path(__file__).parent))

from parse_sistema_dat import parse_sistema_dat_from_zip
from parse_cadic_dat import parse_cadic_dat_from_zip


def extrair_dados_newave(zip_path: str = "deck_newave_2025_11.zip", ano: int = 2025, usar_ano_seguinte: bool = True):
    """
    Extrai dados de MWmed e CAdic dos arquivos NEWAVE.
    
    Args:
        zip_path: Caminho do arquivo ZIP com os arquivos .dat
        ano: Ano para extrair os dados
        usar_ano_seguinte: Se True, busca dados do ano seguinte quando faltam meses
    
    Returns:
        Tupla (MWmed_dict, CAdic_dict)
    """
    print(f"🔍 Extraindo dados do ano {ano}...")
    if usar_ano_seguinte:
        print(f"   ℹ️  Buscando dados de {ano + 1} se faltarem meses em {ano}")
    else:
        print(f"   ℹ️  Mostrando apenas dados disponíveis em {ano}")
    print(f"📦 Arquivo ZIP: {zip_path}\n")
    
    # Extrair MWmed do SISTEMA.DAT
    print("=" * 60)
    print("1️⃣ Extraindo MWmed de SISTEMA.DAT")
    print("=" * 60)
    mwmed_dict = parse_sistema_dat_from_zip(zip_path, ano=ano, usar_ano_seguinte_se_faltar=usar_ano_seguinte)
    
    # Extrair CAdic do C_ADIC.DAT
    print("\n" + "=" * 60)
    print("2️⃣ Extraindo CAdic de C_ADIC.DAT")
    print("=" * 60)
    cadic_dict = parse_cadic_dat_from_zip(zip_path, ano=ano, usar_ano_seguinte_se_faltar=usar_ano_seguinte)
    
    return mwmed_dict, cadic_dict


def formatar_dicionario(d: dict, nome: str, mostrar_todos_meses: bool = False, mes_especifico: int = None) -> str:
    """
    Formata dicionário para exibição Python.
    
    Args:
        d: Dicionário a ser formatado
        nome: Nome da variável
        mostrar_todos_meses: Se True, mostra todos os meses disponíveis; se False, só meses 6-10
        mes_especifico: Se fornecido, mostra apenas esse mês (sobrescreve outros parâmetros)
    
    Returns:
        String formatada
    """
    linhas = [f"{nome} = {{"]
    for sub in ["SE", "S", "NE", "N"]:
        if mes_especifico is not None:
            # Mostrar apenas o mês específico
            if mes_especifico in d[sub] and d[sub][mes_especifico] > 0:
                valor = int(d[sub][mes_especifico])
                valores = f"{mes_especifico}: {valor:>5}"
                linhas.append(f'    "{sub}":   {{{valores}}},')
            else:
                linhas.append(f'    "{sub}":   {{}},')
        elif mostrar_todos_meses:
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
        else:
            # Mostrar apenas meses 6-10 (junho a outubro)
            meses_disponiveis = sorted([m for m in [6, 7, 8, 9, 10] if m in d[sub] and d[sub][m] > 0])
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


def exibir_tabela_completa(mwmed_dict: dict, cadic_dict: dict, ano: int):
    """
    Exibe uma tabela completa com todos os meses e subsistemas.
    
    Args:
        mwmed_dict: Dicionário com valores MWmed
        cadic_dict: Dicionário com valores CAdic
        ano: Ano dos dados
    """
    import calendar
    
    print("\n" + "=" * 80)
    print(f"📅 TABELA COMPLETA - ANO {ano}")
    print("=" * 80)
    
    # Cabeçalho
    meses_nomes = [calendar.month_abbr[i] for i in range(1, 13)]
    print(f"\n{'Subsistema':<12} {'Tipo':<8} " + " ".join([f"{nome:>8}" for nome in meses_nomes]))
    print("-" * 100)
    
    for sub in ["SE", "S", "NE", "N"]:
        linha_mwmed = f"{sub:<12} {'MWmed':<8} "
        linha_cadic = f"{'':<12} {'CAdic':<8} "
        linha_total = f"{'':<12} {'Total':<8} "
        
        for mes in range(1, 13):
            mwmed_val = mwmed_dict[sub].get(mes, 0)
            cadic_val = cadic_dict[sub].get(mes, 0)
            total_val = mwmed_val + cadic_val
            
            linha_mwmed += f"{int(mwmed_val):>8} "
            linha_cadic += f"{int(cadic_val):>8} "
            linha_total += f"{int(total_val):>8} "
        
        print(linha_mwmed)
        print(linha_cadic)
        print(linha_total)
        print("-" * 100)


def main():
    """
    Função principal que executa a extração e exibe os resultados.
    """
    # Configurar parser de argumentos
    parser = argparse.ArgumentParser(
        description='Extrai dados de MWmed e CAdic dos arquivos NEWAVE',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Exemplos:
            %(prog)s                    # Extrai dados de 2025, mostra meses 6-10
            %(prog)s 2026                # Extrai dados de 2026, mostra meses 6-10
            %(prog)s 2026 --mes 6        # Extrai apenas mês 6 (junho) de 2026
            %(prog)s 2025 --mes 11       # Extrai apenas mês 11 (novembro) de 2025
            %(prog)s 2026 --todos-meses  # Extrai todos os meses disponíveis de 2026
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
        '--mes',
        type=int,
        default=None,
        choices=range(1, 13),
        metavar='[1-12]',
        help='Mês específico para extrair (1=janeiro, 12=dezembro). Se não especificado, mostra meses 6-10.'
    )
    parser.add_argument(
        '--zip',
        type=str,
        default=None,
        help='Caminho do arquivo ZIP (padrão: deck_newave_2025_11.zip no diretório pai)'
    )
    parser.add_argument(
        '--todos-meses',
        action='store_true',
        help='Mostrar todos os meses disponíveis (sobrescreve --mes)'
    )
    parser.add_argument(
        '--apenas-ano-solicitado',
        action='store_true',
        help='Mostrar apenas dados do ano solicitado, sem buscar no ano seguinte'
    )
    
    args = parser.parse_args()
    ano = args.ano
    mes_especifico = args.mes
    usar_ano_seguinte = not args.apenas_ano_solicitado
    
    # Determinar caminho do ZIP
    if args.zip:
        zip_path = Path(args.zip)
    else:
        zip_path = Path(__file__).parent.parent / "deck_newave_2025_12.zip"
    
    if not zip_path.exists():
        print(f"❌ Erro: Arquivo {zip_path} não encontrado!")
        print(f"   Diretório atual: {Path.cwd()}")
        return
    
    # Extrair dados
    mwmed_dict, cadic_dict = extrair_dados_newave(str(zip_path), ano=ano, usar_ano_seguinte=usar_ano_seguinte)
    
    # Exibir tabela completa com todos os meses
    exibir_tabela_completa(mwmed_dict, cadic_dict, ano)
    
    # Determinar qual formato usar
    if mes_especifico is not None:
        # Mostrar apenas o mês específico
        meses_pt = {
            1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
            5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
            9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
        }
        nome_mes = meses_pt.get(mes_especifico, f"Mês {mes_especifico}")
        print("\n" + "=" * 60)
        print(f"📊 RESULTADOS FINAIS - MÊS {mes_especifico} ({nome_mes.upper()}) - ANO {ano}")
        print("=" * 60)
        print("\n" + formatar_dicionario(mwmed_dict, "MWmed_dict", mes_especifico=mes_especifico))
        print("\n" + formatar_dicionario(cadic_dict, "CAdic_dict", mes_especifico=mes_especifico))
    else:
        # Verificar se há todos os 12 meses disponíveis
        # Se usar --apenas-ano-solicitado, mostrar todos os meses disponíveis
        # Caso contrário, se tiver todos os meses, mostrar todos; senão, mostrar apenas 6-10
        if args.apenas_ano_solicitado:
            # Mostrar todos os meses disponíveis quando usar apenas ano solicitado
            mostrar_todos = True
        else:
            tem_todos_meses = True
            for sub in ["SE", "S", "NE", "N"]:
                meses_mwmed = sorted([m for m in mwmed_dict[sub].keys() if mwmed_dict[sub][m] > 0])
                meses_cadic = sorted([m for m in cadic_dict[sub].keys() if cadic_dict[sub][m] > 0])
                if len(meses_mwmed) < 12 or len(meses_cadic) < 12:
                    tem_todos_meses = False
                    break
            
            # Se --todos-meses foi especificado, usar True; caso contrário, usar tem_todos_meses
            mostrar_todos = args.todos_meses or tem_todos_meses
        
        print("\n" + "=" * 60)
        print("📊 RESULTADOS FINAIS - DICIONÁRIOS PYTHON")
        print("=" * 60)
        print("\n" + formatar_dicionario(mwmed_dict, "MWmed_dict", mostrar_todos_meses=mostrar_todos))
        print("\n" + formatar_dicionario(cadic_dict, "CAdic_dict", mostrar_todos_meses=mostrar_todos))
    
    # Verificar quais meses estão disponíveis
    print("\n" + "=" * 60)
    print("✅ Verificação de completude")
    print("=" * 60)
    
    for sub in ["SE", "S", "NE", "N"]:
        meses_mwmed = sorted([m for m in mwmed_dict[sub].keys() if mwmed_dict[sub][m] > 0])
        meses_cadic = sorted([m for m in cadic_dict[sub].keys() if cadic_dict[sub][m] > 0])
        
        print(f"\n{sub}:")
        print(f"  MWmed - Meses disponíveis: {meses_mwmed} ({len(meses_mwmed)} meses)")
        print(f"  CAdic - Meses disponíveis: {meses_cadic} ({len(meses_cadic)} meses)")
    
    print("\n" + "=" * 60)
    print("💡 Dica: Você pode copiar os dicionários acima para seu código")
    print("=" * 60)


if __name__ == "__main__":
    main()

