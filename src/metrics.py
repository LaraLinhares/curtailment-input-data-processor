"""
Módulo de métricas para rastreamento de desempenho do processamento de dados.

Este módulo fornece classes para coletar e armazenar métricas durante o processamento,
incluindo:
- Quantidade de dados em cada etapa do pipeline
- Tempo de processamento total e por fonte
- Uso de memória durante o processamento
- Outras métricas relevantes para análise de desempenho
"""

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import psutil


@dataclass
class DataMetrics:
    """Métricas de quantidade de dados em cada etapa do processamento."""
    
    # Dados da fonte (Etapa 1: Extração)
    dados_brutos: int = 0  # Total de registros brutos da fonte (antes de qualquer processamento)
    dados_extraidos: int = 0  # Registros após filtros iniciais (ex: modalidade, período)
    dados_invalidos: int = 0  # Registros descartados por dados inválidos/faltantes
    
    # Limpeza de dados (Etapa 2: Preparação)
    dados_limpos: int = 0  # Registros após limpeza (remoção de outliers, normalização)
    registros_outliers: int = 0  # Número de outliers removidos
    registros_duplicados: int = 0  # Número de duplicatas removidas
    
    # Dados para curva típica (Etapa 3: Modelagem)
    dados_curva_tipica: int = 0  # Registros usados para criar curva típica
    pontos_curva_tipica: int = 0  # Número de pontos na curva típica (ex: 12 meses × 7 dias × 24 horas)
    
    # Dados para projeção (Etapa 4: Projeção)
    dados_projecao: int = 0  # Total de registros projetados (soma de todos os meses)
    dados_projecao_por_mes: Dict[int, int] = field(default_factory=dict)  # Registros por mês
    
    # Dados finais (Etapa 5: Saída)
    dados_finais: int = 0  # Registros no arquivo final processado
    dados_agregados: int = 0  # Registros após agregação (formato melted)
    
    # Qualidade dos dados
    taxa_completude: float = 0.0  # % de dados completos (sem NaN/None)
    taxa_aproveitamento: float = 0.0  # % de dados brutos que chegaram ao final
    
    def calcular_taxas(self):
        """Calcula taxas de qualidade dos dados."""
        # Taxa de aproveitamento
        if self.dados_brutos > 0:
            self.taxa_aproveitamento = (self.dados_finais / self.dados_brutos) * 100
        elif self.dados_curva_tipica > 0:
            # Modo reutilização: usar curva típica como base
            self.taxa_aproveitamento = (self.dados_finais / self.dados_curva_tipica) * 100
        
        # Taxa de completude
        if self.dados_extraidos > 0:
            dados_validos = self.dados_extraidos - self.dados_invalidos
            self.taxa_completude = (dados_validos / self.dados_extraidos) * 100
        elif self.dados_curva_tipica > 0:
            # Modo reutilização: assumir 100% de completude (curva já validada)
            self.taxa_completude = 100.0
    
    def to_dict(self) -> Dict:
        """Converte as métricas para dicionário."""
        self.calcular_taxas()
        return {
            # Extração
            "dados_brutos": self.dados_brutos,
            "dados_extraidos": self.dados_extraidos,
            "dados_invalidos": self.dados_invalidos,
            
            # Limpeza
            "dados_limpos": self.dados_limpos,
            "registros_outliers": self.registros_outliers,
            "registros_duplicados": self.registros_duplicados,
            
            # Modelagem
            "dados_curva_tipica": self.dados_curva_tipica,
            "pontos_curva_tipica": self.pontos_curva_tipica,
            
            # Projeção
            "dados_projecao": self.dados_projecao,
            "dados_projecao_por_mes": self.dados_projecao_por_mes,
            
            # Saída
            "dados_finais": self.dados_finais,
            "dados_agregados": self.dados_agregados,
            
            # Qualidade
            "taxa_completude_pct": round(self.taxa_completude, 2),
            "taxa_aproveitamento_pct": round(self.taxa_aproveitamento, 2),
        }


@dataclass
class PerformanceMetrics:
    """Métricas de desempenho do processamento."""
    
    # Tempo total
    tempo_inicio: Optional[float] = None
    tempo_fim: Optional[float] = None
    tempo_total_segundos: float = 0.0
    
    # Tempo por etapa (em segundos)
    tempo_extracao: float = 0.0
    tempo_limpeza: float = 0.0
    tempo_curva_tipica: float = 0.0
    tempo_projecao: float = 0.0
    tempo_agregacao: float = 0.0
    
    # Métricas específicas de curva típica (separadas)
    curva_tipica_inicio: Optional[float] = None
    curva_tipica_fim: Optional[float] = None
    curva_tipica_memoria_antes_mb: float = 0.0
    curva_tipica_memoria_depois_mb: float = 0.0
    curva_tipica_memoria_delta_mb: float = 0.0
    curva_tipica_throughput: float = 0.0  # Pontos gerados por segundo
    
    # Memória
    memoria_inicial_mb: float = 0.0
    memoria_final_mb: float = 0.0
    memoria_pico_mb: float = 0.0
    memoria_media_mb: float = 0.0
    memoria_minima_mb: float = float('inf')
    
    # Amostras de memória durante processamento
    _amostras_memoria: List[float] = field(default_factory=list)
    _timestamps_memoria: List[float] = field(default_factory=list)
    
    # CPU
    cpu_percent_media: float = 0.0
    cpu_percent_pico: float = 0.0
    _amostras_cpu: List[float] = field(default_factory=list)
    
    # I/O
    bytes_lidos: int = 0
    bytes_escritos: int = 0
    arquivos_lidos: int = 0
    arquivos_escritos: int = 0
    
    # Throughput (registros por segundo)
    throughput_registros_por_segundo: float = 0.0
    
    def iniciar(self):
        """Inicia o rastreamento de métricas."""
        self.tempo_inicio = time.time()
        process = psutil.Process(os.getpid())
        self.memoria_inicial_mb = process.memory_info().rss / 1024 / 1024
        self._amostras_memoria = [self.memoria_inicial_mb]
        self._timestamps_memoria = [time.time()]
        
        # CPU baseline
        self._amostras_cpu = [process.cpu_percent(interval=0.1)]
    
    def amostrar_memoria(self):
        """Coleta uma amostra de uso de memória e CPU."""
        process = psutil.Process(os.getpid())
        
        # Memória
        memoria_atual = process.memory_info().rss / 1024 / 1024
        self._amostras_memoria.append(memoria_atual)
        self._timestamps_memoria.append(time.time())
        self.memoria_pico_mb = max(self.memoria_pico_mb, memoria_atual)
        self.memoria_minima_mb = min(self.memoria_minima_mb, memoria_atual)
        
        # CPU
        cpu_atual = process.cpu_percent(interval=0.1)
        self._amostras_cpu.append(cpu_atual)
        self.cpu_percent_pico = max(self.cpu_percent_pico, cpu_atual)
    
    def iniciar_curva_tipica(self):
        """Inicia medição específica da criação de curva típica."""
        self.curva_tipica_inicio = time.time()
        process = psutil.Process(os.getpid())
        self.curva_tipica_memoria_antes_mb = process.memory_info().rss / 1024 / 1024
    
    def finalizar_curva_tipica(self, pontos_gerados: int = 0):
        """
        Finaliza medição da criação de curva típica.
        
        Args:
            pontos_gerados: Número de pontos na curva típica gerada
        """
        if self.curva_tipica_inicio is None:
            return
        
        self.curva_tipica_fim = time.time()
        self.tempo_curva_tipica = self.curva_tipica_fim - self.curva_tipica_inicio
        
        process = psutil.Process(os.getpid())
        self.curva_tipica_memoria_depois_mb = process.memory_info().rss / 1024 / 1024
        self.curva_tipica_memoria_delta_mb = (
            self.curva_tipica_memoria_depois_mb - self.curva_tipica_memoria_antes_mb
        )
        
        # Calcular throughput (pontos por segundo)
        if self.tempo_curva_tipica > 0 and pontos_gerados > 0:
            self.curva_tipica_throughput = pontos_gerados / self.tempo_curva_tipica
    
    def finalizar(self, total_registros: int = 0):
        """
        Finaliza o rastreamento e calcula métricas finais.
        
        Args:
            total_registros: Total de registros PROCESSADOS (não gerados).
                            Deve ser dados_extraidos ou dados_brutos para calcular
                            throughput de processamento corretamente.
        """
        self.tempo_fim = time.time()
        self.tempo_total_segundos = self.tempo_fim - self.tempo_inicio
        
        process = psutil.Process(os.getpid())
        self.memoria_final_mb = process.memory_info().rss / 1024 / 1024
        
        # Calcular médias
        if self._amostras_memoria:
            self.memoria_media_mb = sum(self._amostras_memoria) / len(self._amostras_memoria)
        
        if self._amostras_cpu:
            self.cpu_percent_media = sum(self._amostras_cpu) / len(self._amostras_cpu)
        
        # Calcular throughput (registros processados por segundo)
        if self.tempo_total_segundos > 0 and total_registros > 0:
            self.throughput_registros_por_segundo = total_registros / self.tempo_total_segundos
    
    def obter_taxa_crescimento_memoria(self) -> float:
        """Calcula taxa de crescimento de memória por segundo (MB/s)."""
        if self.tempo_total_segundos > 0:
            delta_memoria = self.memoria_final_mb - self.memoria_inicial_mb
            return delta_memoria / self.tempo_total_segundos
        return 0.0
    
    def obter_eficiencia_memoria(self, total_registros: int) -> float:
        """Calcula eficiência de memória (MB por milhão de registros)."""
        if total_registros > 0:
            return (self.memoria_pico_mb / total_registros) * 1_000_000
        return 0.0
    
    def to_dict(self) -> Dict:
        """Converte as métricas para dicionário."""
        return {
            # Tempo total
            "tempo_total_segundos": round(self.tempo_total_segundos, 2),
            "tempo_total_minutos": round(self.tempo_total_segundos / 60, 2),
            "tempo_total_horas": round(self.tempo_total_segundos / 3600, 2),
            
            # Tempo por etapa
            "tempo_extracao_seg": round(self.tempo_extracao, 2),
            "tempo_limpeza_seg": round(self.tempo_limpeza, 2),
            "tempo_curva_tipica_seg": round(self.tempo_curva_tipica, 2),
            "tempo_projecao_seg": round(self.tempo_projecao, 2),
            "tempo_agregacao_seg": round(self.tempo_agregacao, 2),
            
            # Distribuição de tempo (%)
            "pct_tempo_extracao": round((self.tempo_extracao / self.tempo_total_segundos * 100) if self.tempo_total_segundos > 0 else 0, 2),
            "pct_tempo_limpeza": round((self.tempo_limpeza / self.tempo_total_segundos * 100) if self.tempo_total_segundos > 0 else 0, 2),
            "pct_tempo_curva_tipica": round((self.tempo_curva_tipica / self.tempo_total_segundos * 100) if self.tempo_total_segundos > 0 else 0, 2),
            "pct_tempo_projecao": round((self.tempo_projecao / self.tempo_total_segundos * 100) if self.tempo_total_segundos > 0 else 0, 2),
            
            # Métricas específicas de curva típica (SEPARADAS)
            "curva_tipica_tempo_seg": round(self.tempo_curva_tipica, 2),
            "curva_tipica_memoria_antes_mb": round(self.curva_tipica_memoria_antes_mb, 2),
            "curva_tipica_memoria_depois_mb": round(self.curva_tipica_memoria_depois_mb, 2),
            "curva_tipica_memoria_delta_mb": round(self.curva_tipica_memoria_delta_mb, 2),
            "curva_tipica_throughput_pontos_por_seg": round(self.curva_tipica_throughput, 2),
            
            # Memória
            "memoria_inicial_mb": round(self.memoria_inicial_mb, 2),
            "memoria_final_mb": round(self.memoria_final_mb, 2),
            "memoria_pico_mb": round(self.memoria_pico_mb, 2),
            "memoria_media_mb": round(self.memoria_media_mb, 2),
            "memoria_minima_mb": round(self.memoria_minima_mb, 2) if self.memoria_minima_mb != float('inf') else 0,
            "memoria_delta_mb": round(self.memoria_final_mb - self.memoria_inicial_mb, 2),
            "memoria_crescimento_mb_por_seg": round(self.obter_taxa_crescimento_memoria(), 4),
            
            # CPU
            "cpu_percent_media": round(self.cpu_percent_media, 2),
            "cpu_percent_pico": round(self.cpu_percent_pico, 2),
            
            # I/O
            "bytes_lidos": self.bytes_lidos,
            "bytes_escritos": self.bytes_escritos,
            "mb_lidos": round(self.bytes_lidos / 1024 / 1024, 2),
            "mb_escritos": round(self.bytes_escritos / 1024 / 1024, 2),
            "arquivos_lidos": self.arquivos_lidos,
            "arquivos_escritos": self.arquivos_escritos,
            
            # Throughput
            "throughput_registros_por_segundo": round(self.throughput_registros_por_segundo, 2),
            "throughput_registros_por_minuto": round(self.throughput_registros_por_segundo * 60, 2),
        }


@dataclass
class SourceMetrics:
    """Métricas completas para uma fonte de dados."""
    
    nome_fonte: str
    data_metrics: DataMetrics = field(default_factory=DataMetrics)
    performance_metrics: PerformanceMetrics = field(default_factory=PerformanceMetrics)
    
    # Informações adicionais
    arquivos_processados: List[str] = field(default_factory=list)
    erros: List[str] = field(default_factory=list)
    avisos: List[str] = field(default_factory=list)
    
    # Estatísticas de qualidade
    estatisticas_qualidade: Dict[str, float] = field(default_factory=dict)
    
    # Metadados
    versao_fonte: str = ""  # Versão dos dados da fonte (ex: "PMO_NOV_2025")
    data_fonte: str = ""  # Data de referência dos dados
    
    def adicionar_estatistica_qualidade(self, nome: str, valor: float):
        """Adiciona uma estatística de qualidade dos dados."""
        self.estatisticas_qualidade[nome] = valor
    
    def calcular_score_qualidade(self) -> float:
        """
        Calcula um score de qualidade geral (0-100).
        
        Considera:
        - Taxa de completude
        - Taxa de aproveitamento
        - Número de erros
        - Número de avisos
        """
        score = 100.0
        
        # Penalizar por completude baixa (peso: 30%)
        if self.data_metrics.taxa_completude < 100:
            score -= (100 - self.data_metrics.taxa_completude) * 0.3
        
        # Penalizar por aproveitamento baixo (peso: 30%)
        if self.data_metrics.taxa_aproveitamento < 100:
            score -= (100 - self.data_metrics.taxa_aproveitamento) * 0.3
        
        # Penalizar por erros (peso: 30%)
        if len(self.erros) > 0:
            score -= min(len(self.erros) * 5, 30)  # Máximo 30 pontos
        
        # Penalizar por avisos (peso: 10%)
        if len(self.avisos) > 0:
            score -= min(len(self.avisos) * 2, 10)  # Máximo 10 pontos
        
        return max(0.0, score)
    
    def to_dict(self) -> Dict:
        """Converte todas as métricas para dicionário."""
        return {
            "nome_fonte": self.nome_fonte,
            "versao_fonte": self.versao_fonte,
            "data_fonte": self.data_fonte,
            
            # Métricas de dados
            "data_metrics": self.data_metrics.to_dict(),
            
            # Métricas de performance
            "performance_metrics": self.performance_metrics.to_dict(),
            
            # Arquivos
            "arquivos_processados": self.arquivos_processados,
            "num_arquivos_processados": len(self.arquivos_processados),
            
            # Qualidade
            "num_erros": len(self.erros),
            "num_avisos": len(self.avisos),
            "lista_erros": self.erros,
            "lista_avisos": self.avisos,
            "estatisticas_qualidade": self.estatisticas_qualidade,
            "score_qualidade": round(self.calcular_score_qualidade(), 2),
        }


@dataclass
class ProcessingMetrics:
    """Métricas agregadas de todo o processamento."""
    
    timestamp_inicio: str = field(default_factory=lambda: datetime.now().isoformat())
    timestamp_fim: Optional[str] = None
    
    # Configuração do processamento
    cenario: str = ""  # Ex: "1_mes", "3_meses", "6_meses", "12_semanas_tipicas"
    periodo_inicio: Optional[str] = None
    periodo_fim: Optional[str] = None
    num_meses_processados: int = 0
    
    # Métricas por fonte
    metricas_fontes: Dict[str, SourceMetrics] = field(default_factory=dict)
    
    # Métricas totais agregadas
    tempo_total_segundos: float = 0.0
    dados_totais_brutos: int = 0
    dados_totais_finais: int = 0
    dados_totais_agregados: int = 0
    
    # Estatísticas comparativas entre fontes
    fonte_mais_rapida: str = ""
    fonte_mais_lenta: str = ""
    fonte_maior_volume: str = ""
    fonte_menor_volume: str = ""
    fonte_maior_qualidade: str = ""
    
    # Informações do sistema
    info_sistema: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """Inicializa informações do sistema."""
        import platform
        self.info_sistema = {
            "python_version": platform.python_version(),
            "sistema_operacional": platform.system(),
            "arquitetura": platform.machine(),
            "processador": platform.processor(),
            "num_cpus": str(psutil.cpu_count()),
            "memoria_total_gb": f"{psutil.virtual_memory().total / (1024**3):.2f}",
        }
    
    def adicionar_fonte(self, nome_fonte: str) -> SourceMetrics:
        """Adiciona uma nova fonte para rastreamento."""
        if nome_fonte not in self.metricas_fontes:
            self.metricas_fontes[nome_fonte] = SourceMetrics(nome_fonte=nome_fonte)
        return self.metricas_fontes[nome_fonte]
    
    def obter_fonte(self, nome_fonte: str) -> Optional[SourceMetrics]:
        """Obtém métricas de uma fonte específica."""
        return self.metricas_fontes.get(nome_fonte)
    
    def calcular_estatisticas_comparativas(self):
        """Calcula estatísticas comparativas entre fontes."""
        if not self.metricas_fontes:
            return
        
        # Encontrar fonte mais rápida/lenta
        tempos = {nome: m.performance_metrics.tempo_total_segundos 
                 for nome, m in self.metricas_fontes.items()}
        if tempos:
            self.fonte_mais_rapida = min(tempos, key=tempos.get)
            self.fonte_mais_lenta = max(tempos, key=tempos.get)
        
        # Encontrar fonte com maior/menor volume
        volumes = {nome: m.data_metrics.dados_finais 
                  for nome, m in self.metricas_fontes.items()}
        if volumes:
            self.fonte_maior_volume = max(volumes, key=volumes.get)
            self.fonte_menor_volume = min(volumes, key=volumes.get)
        
        # Encontrar fonte com maior qualidade
        qualidades = {nome: m.calcular_score_qualidade() 
                     for nome, m in self.metricas_fontes.items()}
        if qualidades:
            self.fonte_maior_qualidade = max(qualidades, key=qualidades.get)
    
    def obter_estatisticas_agregadas(self) -> Dict:
        """Calcula estatísticas agregadas de todas as fontes."""
        if not self.metricas_fontes:
            return {}
        
        # Coletar métricas de todas as fontes
        tempos = [m.performance_metrics.tempo_total_segundos for m in self.metricas_fontes.values()]
        memorias_pico = [m.performance_metrics.memoria_pico_mb for m in self.metricas_fontes.values()]
        throughputs = [m.performance_metrics.throughput_registros_por_segundo for m in self.metricas_fontes.values()]
        taxas_completude = [m.data_metrics.taxa_completude for m in self.metricas_fontes.values()]
        taxas_aproveitamento = [m.data_metrics.taxa_aproveitamento for m in self.metricas_fontes.values()]
        scores_qualidade = [m.calcular_score_qualidade() for m in self.metricas_fontes.values()]
        
        return {
            # Tempo
            "tempo_medio_por_fonte_seg": round(sum(tempos) / len(tempos), 2) if tempos else 0,
            "tempo_min_seg": round(min(tempos), 2) if tempos else 0,
            "tempo_max_seg": round(max(tempos), 2) if tempos else 0,
            "desvio_padrao_tempo_seg": round(pd.Series(tempos).std(), 2) if len(tempos) > 1 else 0,
            
            # Memória
            "memoria_pico_media_mb": round(sum(memorias_pico) / len(memorias_pico), 2) if memorias_pico else 0,
            "memoria_pico_max_mb": round(max(memorias_pico), 2) if memorias_pico else 0,
            
            # Throughput
            "throughput_medio_reg_por_seg": round(sum(throughputs) / len(throughputs), 2) if throughputs else 0,
            "throughput_max_reg_por_seg": round(max(throughputs), 2) if throughputs else 0,
            
            # Qualidade
            "taxa_completude_media_pct": round(sum(taxas_completude) / len(taxas_completude), 2) if taxas_completude else 0,
            "taxa_aproveitamento_media_pct": round(sum(taxas_aproveitamento) / len(taxas_aproveitamento), 2) if taxas_aproveitamento else 0,
            "score_qualidade_medio": round(sum(scores_qualidade) / len(scores_qualidade), 2) if scores_qualidade else 0,
            "score_qualidade_min": round(min(scores_qualidade), 2) if scores_qualidade else 0,
        }
    
    def finalizar(self):
        """Finaliza o rastreamento e calcula métricas agregadas."""
        self.timestamp_fim = datetime.now().isoformat()
        
        # Calcular tempo total (soma de todos os processamentos)
        self.tempo_total_segundos = sum(
            m.performance_metrics.tempo_total_segundos 
            for m in self.metricas_fontes.values()
        )
        
        # Calcular totais de dados
        self.dados_totais_brutos = sum(
            m.data_metrics.dados_brutos 
            for m in self.metricas_fontes.values()
        )
        
        self.dados_totais_finais = sum(
            m.data_metrics.dados_finais 
            for m in self.metricas_fontes.values()
        )
        
        self.dados_totais_agregados = sum(
            m.data_metrics.dados_agregados 
            for m in self.metricas_fontes.values()
        )
        
        # Calcular estatísticas comparativas
        self.calcular_estatisticas_comparativas()
    
    def to_dict(self) -> Dict:
        """Converte todas as métricas para dicionário."""
        estatisticas = self.obter_estatisticas_agregadas()
        
        return {
            # Metadados
            "timestamp_inicio": self.timestamp_inicio,
            "timestamp_fim": self.timestamp_fim,
            "cenario": self.cenario,
            "periodo_inicio": self.periodo_inicio,
            "periodo_fim": self.periodo_fim,
            "num_meses_processados": self.num_meses_processados,
            
            # Tempo total
            "tempo_total_segundos": round(self.tempo_total_segundos, 2),
            "tempo_total_minutos": round(self.tempo_total_segundos / 60, 2),
            "tempo_total_horas": round(self.tempo_total_segundos / 3600, 2),
            
            # Dados totais
            "dados_totais_brutos": self.dados_totais_brutos,
            "dados_totais_finais": self.dados_totais_finais,
            "dados_totais_agregados": self.dados_totais_agregados,
            "taxa_aproveitamento_global_pct": round(
                (self.dados_totais_finais / self.dados_totais_brutos * 100) 
                if self.dados_totais_brutos > 0 else 0, 2
            ),
            
            # Comparações entre fontes
            "fonte_mais_rapida": self.fonte_mais_rapida,
            "fonte_mais_lenta": self.fonte_mais_lenta,
            "fonte_maior_volume": self.fonte_maior_volume,
            "fonte_menor_volume": self.fonte_menor_volume,
            "fonte_maior_qualidade": self.fonte_maior_qualidade,
            
            # Estatísticas agregadas
            "estatisticas_agregadas": estatisticas,
            
            # Métricas por fonte
            "metricas_por_fonte": {
                nome: metricas.to_dict() 
                for nome, metricas in self.metricas_fontes.items()
            },
            
            # Informações do sistema
            "info_sistema": self.info_sistema,
        }
    
    def imprimir_resumo(self):
        """Imprime um resumo formatado das métricas."""
        print("\n" + "=" * 80)
        print(f"RESUMO DAS MÉTRICAS DE PROCESSAMENTO - Cenário: {self.cenario}")
        print("=" * 80)
        
        print(f"\n📅 Período: {self.periodo_inicio} a {self.periodo_fim} ({self.num_meses_processados} meses)")
        print(f"⏰ Tempo total: {self.tempo_total_segundos/60:.2f} minutos ({self.tempo_total_segundos/3600:.2f} horas)")
        print(f"📊 Dados brutos: {self.dados_totais_brutos:,} registros")
        print(f"📊 Dados finais: {self.dados_totais_finais:,} registros")
        print(f"📊 Dados agregados: {self.dados_totais_agregados:,} registros")
        
        if self.dados_totais_brutos > 0:
            taxa = (self.dados_totais_finais / self.dados_totais_brutos) * 100
            print(f"✅ Taxa de aproveitamento global: {taxa:.2f}%")
        
        print("\n" + "-" * 80)
        print("COMPARAÇÃO ENTRE FONTES")
        print("-" * 80)
        print(f"🏃 Fonte mais rápida: {self.fonte_mais_rapida}")
        print(f"🐢 Fonte mais lenta: {self.fonte_mais_lenta}")
        print(f"📈 Maior volume de dados: {self.fonte_maior_volume}")
        print(f"📉 Menor volume de dados: {self.fonte_menor_volume}")
        print(f"⭐ Maior qualidade: {self.fonte_maior_qualidade}")
        
        # Estatísticas agregadas
        stats = self.obter_estatisticas_agregadas()
        if stats:
            print("\n" + "-" * 80)
            print("ESTATÍSTICAS AGREGADAS")
            print("-" * 80)
            print(f"⏱️  Tempo médio por fonte: {stats['tempo_medio_por_fonte_seg']:.2f}s")
            print(f"💾 Memória pico média: {stats['memoria_pico_media_mb']:.1f} MB")
            print(f"⚡ Throughput médio: {stats['throughput_medio_reg_por_seg']:.2f} reg/s")
            print(f"✅ Completude média: {stats['taxa_completude_media_pct']:.2f}%")
            print(f"✅ Aproveitamento médio: {stats['taxa_aproveitamento_media_pct']:.2f}%")
            print(f"⭐ Score de qualidade médio: {stats['score_qualidade_medio']:.2f}/100")
        
        print("\n" + "-" * 80)
        print("MÉTRICAS POR FONTE DE DADOS")
        print("-" * 80)
        
        for nome_fonte, metricas in self.metricas_fontes.items():
            print(f"\n🔹 {nome_fonte.upper()}")
            
            # Dados
            dm = metricas.data_metrics
            print(f"   📥 Dados brutos: {dm.dados_brutos:,}")
            print(f"   📥 Dados extraídos: {dm.dados_extraidos:,}")
            print(f"   🧹 Dados limpos: {dm.dados_limpos:,}")
            print(f"   📈 Dados curva típica: {dm.dados_curva_tipica:,}")
            print(f"   🔮 Dados projeção: {dm.dados_projecao:,}")
            print(f"   📤 Dados finais: {dm.dados_finais:,}")
            print(f"   ✅ Taxa completude: {dm.taxa_completude:.2f}%")
            print(f"   ✅ Taxa aproveitamento: {dm.taxa_aproveitamento:.2f}%")
            
            # Desempenho
            pm = metricas.performance_metrics
            print(f"   ⏱️  Tempo total: {pm.tempo_total_segundos:.2f}s ({pm.tempo_total_segundos/60:.2f}min)")
            print(f"   💾 Memória pico: {pm.memoria_pico_mb:.1f} MB")
            print(f"   💻 CPU média: {pm.cpu_percent_media:.1f}%")
            print(f"   ⚡ Throughput: {pm.throughput_registros_por_segundo:.2f} reg/s")
            print(f"   ⭐ Score qualidade: {metricas.calcular_score_qualidade():.2f}/100")
            print(f"   ⚠️  Avisos: {len(metricas.avisos)}")
            print(f"   ❌ Erros: {len(metricas.erros)}")
        
        print("\n" + "=" * 80)
        print("FIM DO RESUMO")
        print("=" * 80 + "\n")


