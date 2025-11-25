"""
Testes de execução para diferentes cenários de processamento.

Este módulo implementa testes para os seguintes cenários:
1. Processamento de dados de 1 mês
2. Processamento de dados de 3 meses
3. Processamento de dados de 6 meses
4. Processamento de dados de 12 semanas típicas de 1 ano

Para cada teste, são impressas métricas detalhadas:
- Quantidade de dados processados desde a extração da fonte
- Quantidade de dados utilizados na criação da curva típica
- Quantidade de dados utilizados na projeção de geração
- Quantidade de dados no arquivo final processado
- Tempo de processamento total e por fonte
- Uso de memória durante o processamento
"""

import json
import sys
import unittest
from datetime import datetime
from pathlib import Path

import pandas as pd

# Adicionar diretório pai ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from main import processar_curtailment


class TestProcessamentoScenarios(unittest.TestCase):
    """Testes para diferentes cenários de processamento."""
    
    @classmethod
    def setUpClass(cls):
        """Configuração inicial dos testes."""
        cls.base_dir = Path(__file__).parent.parent
        cls.ano = 2026
        cls.output_base_dir = cls.base_dir / "outputs" / "testes"
        cls.output_base_dir.mkdir(parents=True, exist_ok=True)
        
        # Armazenar resultados de todos os testes para comparação
        cls.resultados_testes = []
    
    def _salvar_resultado_teste(self, cenario: str, metricas):
        """Salva resultado do teste para análise comparativa."""
        resultado = {
            "cenario": cenario,
            "timestamp": datetime.now().isoformat(),
            "metricas": metricas.to_dict()
        }
        self.resultados_testes.append(resultado)
    
    def test_1_cenario_1_mes(self):
        """
        Teste 1: Processamento de dados de 1 mês.
        
        Este teste processa dados de um único mês (janeiro/2026) e coleta
        métricas de desempenho e quantidade de dados.
        """
        print("\n" + "="*80)
        print("TESTE 1: PROCESSAMENTO DE 1 MÊS")
        print("="*80)
        
        cenario = "1_mes"
        meses = [1]  # Janeiro
        output_dir = self.output_base_dir / cenario
        
        metricas = processar_curtailment(
            base_dir=self.base_dir,
            meses=meses,
            ano=self.ano,
            output_dir=output_dir,
            cenario=cenario
        )
        
        self._salvar_resultado_teste(cenario, metricas)
        
        # Verificações
        self.assertIsNotNone(metricas)
        self.assertGreater(len(metricas.metricas_fontes), 0)
        self.assertGreater(metricas.dados_totais_finais, 0)
        
        print(f"\n✅ Teste 1 concluído com sucesso!")
    
    def test_2_cenario_3_meses(self):
        """
        Teste 2: Processamento de dados de 3 meses.
        
        Este teste processa dados de três meses consecutivos (jan-mar/2026)
        e compara as métricas com o cenário de 1 mês.
        """
        print("\n" + "="*80)
        print("TESTE 2: PROCESSAMENTO DE 3 MESES")
        print("="*80)
        
        cenario = "3_meses"
        meses = [1, 2, 3]  # Janeiro a Março
        output_dir = self.output_base_dir / cenario
        
        metricas = processar_curtailment(
            base_dir=self.base_dir,
            meses=meses,
            ano=self.ano,
            output_dir=output_dir,
            cenario=cenario
        )
        
        self._salvar_resultado_teste(cenario, metricas)
        
        # Verificações
        self.assertIsNotNone(metricas)
        self.assertGreater(len(metricas.metricas_fontes), 0)
        self.assertGreater(metricas.dados_totais_finais, 0)
        
        print(f"\n✅ Teste 2 concluído com sucesso!")
    
    def test_3_cenario_6_meses(self):
        """
        Teste 3: Processamento de dados de 6 meses.
        
        Este teste processa dados de seis meses (jan-jun/2026) e analisa
        o impacto no desempenho e uso de memória.
        """
        print("\n" + "="*80)
        print("TESTE 3: PROCESSAMENTO DE 6 MESES")
        print("="*80)
        
        cenario = "6_meses"
        meses = [1, 2, 3, 4, 5, 6]  # Janeiro a Junho
        output_dir = self.output_base_dir / cenario
        
        metricas = processar_curtailment(
            base_dir=self.base_dir,
            meses=meses,
            ano=self.ano,
            output_dir=output_dir,
            cenario=cenario
        )
        
        self._salvar_resultado_teste(cenario, metricas)
        
        # Verificações
        self.assertIsNotNone(metricas)
        self.assertGreater(len(metricas.metricas_fontes), 0)
        self.assertGreater(metricas.dados_totais_finais, 0)
        
        print(f"\n✅ Teste 3 concluído com sucesso!")
    
    def test_4_cenario_12_semanas_tipicas(self):
        """
        Teste 4: Processamento de 12 semanas típicas de 1 ano.
        
        Este teste processa uma semana típica de cada mês do ano (12 meses),
        representando um ano completo de dados típicos.
        """
        print("\n" + "="*80)
        print("TESTE 4: PROCESSAMENTO DE 12 SEMANAS TÍPICAS (1 ANO)")
        print("="*80)
        
        cenario = "12_semanas_tipicas"
        meses = list(range(1, 13))  # Todos os 12 meses
        output_dir = self.output_base_dir / cenario
        
        metricas = processar_curtailment(
            base_dir=self.base_dir,
            meses=meses,
            ano=self.ano,
            output_dir=output_dir,
            cenario=cenario
        )
        
        self._salvar_resultado_teste(cenario, metricas)
        
        # Verificações
        self.assertIsNotNone(metricas)
        self.assertGreater(len(metricas.metricas_fontes), 0)
        self.assertGreater(metricas.dados_totais_finais, 0)
        
        print(f"\n✅ Teste 4 concluído com sucesso!")
    
    @classmethod
    def tearDownClass(cls):
        """
        Gera relatório comparativo de todos os testes.
        
        Este relatório consolida as métricas de todos os cenários testados
        para facilitar a análise comparativa.
        """
        print("\n" + "="*80)
        print("RELATÓRIO COMPARATIVO DE TODOS OS CENÁRIOS")
        print("="*80)
        
        # Criar DataFrame comparativo
        dados_comparacao = []
        
        for resultado in cls.resultados_testes:
            cenario = resultado["cenario"]
            metricas = resultado["metricas"]
            
            linha = {
                "Cenário": cenario,
                "Tempo Total (min)": metricas["tempo_total_minutos"],
                "Dados Totais": metricas["dados_totais_finais"],
            }
            
            # Adicionar métricas por fonte
            for nome_fonte, metricas_fonte in metricas["metricas_por_fonte"].items():
                pm = metricas_fonte["performance_metrics"]
                dm = metricas_fonte["data_metrics"]
                
                linha[f"{nome_fonte}_tempo_min"] = pm["tempo_total_minutos"]
                linha[f"{nome_fonte}_memoria_mb"] = pm["memoria_pico_mb"]
                linha[f"{nome_fonte}_dados"] = dm["dados_finais"]
            
            dados_comparacao.append(linha)
        
        # Criar e salvar DataFrame
        df_comparacao = pd.DataFrame(dados_comparacao)
        
        # Salvar em CSV
        arquivo_comparacao = cls.output_base_dir / "comparacao_cenarios.csv"
        df_comparacao.to_csv(arquivo_comparacao, index=False, sep=';', decimal=',')
        print(f"\n💾 Relatório comparativo salvo em: {arquivo_comparacao}")
        
        # Imprimir resumo
        print("\n📊 RESUMO COMPARATIVO:")
        print("-" * 80)
        print(df_comparacao.to_string(index=False))
        
        # Salvar resultados detalhados em JSON
        arquivo_json = cls.output_base_dir / "resultados_completos.json"
        with open(arquivo_json, 'w', encoding='utf-8') as f:
            json.dump(cls.resultados_testes, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Resultados completos salvos em: {arquivo_json}")
        
        print("\n" + "="*80)
        print("TODOS OS TESTES CONCLUÍDOS COM SUCESSO!")
        print("="*80 + "\n")


def run_all_tests():
    """Executa todos os testes de cenário."""
    # Criar test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestProcessamentoScenarios)
    
    # Executar testes
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result


if __name__ == "__main__":
    # Executar todos os testes
    result = run_all_tests()
    
    # Retornar código de saída apropriado
    sys.exit(0 if result.wasSuccessful() else 1)

