"""
Processadores de dados integrados para o sistema de curtailment.

Pipeline completo automatizado:
1. Extração de dados históricos
2. Criação de curva típica
3. Projeção de geração
4. Agregação final

Cada processador implementa todo o pipeline e coleta métricas detalhadas.
"""

import calendar
import locale
import os
import re
import sys
import unicodedata
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Adicionar diretório pai ao sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.metrics import SourceMetrics

# Configuração de locale
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, "pt_BR")
    except locale.Error:
        pass


# ============================================================================
# CONFIGURAÇÃO DO MODO DE PROCESSAMENTO
# ============================================================================

class PeriodMode(Enum):
    """
    Define o modo de processamento dos dados.
    
    - SEMANA_TIPICA: Processa apenas 7 dias (uma semana típica) por mês
    - PERIODO_COMPLETO: Processa todos os dias do período especificado
    """
    SEMANA_TIPICA = "semana_tipica"
    PERIODO_COMPLETO = "periodo_completo"


@dataclass
class ProcessingConfig:
    """
    Configuração de processamento.
    
    Attributes:
        mode: Modo de processamento (SEMANA_TIPICA ou PERIODO_COMPLETO)
        dias_por_mes: Número de dias a processar por mês (usado apenas em PERIODO_COMPLETO)
                      None = processar mês completo
    """
    mode: PeriodMode = PeriodMode.SEMANA_TIPICA
    dias_por_mes: Optional[int] = None  # None = mês completo
    
    def get_description(self) -> str:
        """Retorna descrição legível da configuração."""
        if self.mode == PeriodMode.SEMANA_TIPICA:
            return "Semana Típica (7 dias por mês)"
        else:
            if self.dias_por_mes is None:
                return "Período Completo (mês inteiro)"
            else:
                return f"Período Completo ({self.dias_por_mes} dias por mês)"


# ============================================================================
# CONFIGURAÇÃO GLOBAL - EDITE AQUI PARA MUDAR O COMPORTAMENTO
# ============================================================================

# Exemplos de uso:
# 
# 1. Para processar SEMANAS TÍPICAS (padrão):
#    DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.SEMANA_TIPICA)
#
# 2. Para processar MÊS COMPLETO (30-31 dias):
#    DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.PERIODO_COMPLETO, dias_por_mes=None)
#
# 3. Para processar 30 DIAS FIXOS por mês:
#    DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.PERIODO_COMPLETO, dias_por_mes=30)
#
# 4. Para processar 90 DIAS (para cenário de 3 meses):
#    DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.PERIODO_COMPLETO, dias_por_mes=90)

DEFAULT_CONFIG = ProcessingConfig(
    mode=PeriodMode.PERIODO_COMPLETO,  # Altere para PeriodMode.PERIODO_COMPLETO se necessário
    dias_por_mes=None  # Altere para número de dias específico se necessário
)

# ============================================================================
# CONFIGURAÇÃO DE REUTILIZAÇÃO DE CURVAS TÍPICAS
# ============================================================================
#
# Se True, o sistema tentará REUTILIZAR curvas típicas já existentes ao invés
# de recriá-las. Isso acelera MUITO o processamento, pois pula a etapa de:
# - Download de dados históricos (ONS)
# - Limpeza e normalização
# - Criação da curva típica
#
# Quando usar REUSAR_CURVAS_TIPICAS = True:
# - Para testes rápidos após já ter criado as curvas
# - Para focar nas métricas de PROJEÇÃO e AGREGAÇÃO
# - Quando os dados históricos não mudaram
#
# Quando usar REUSAR_CURVAS_TIPICAS = False:
# - Primeira execução ou quando dados históricos foram atualizados
# - Para gerar métricas COMPLETAS incluindo criação de curvas
# - Para pesquisa/TCC onde o processo completo é importante

REUSAR_CURVAS_TIPICAS = True  # Altere para True para reutilizar curvas existentes

# ============================================================================

# Constantes
SUBSISTEMAS = ["SE", "S", "NE", "N"]
DAY_MAP_EN_TO_PT = {
    'Monday': 'SEGUNDA', 'Tuesday': 'TERÇA', 'Wednesday': 'QUARTA',
    'Thursday': 'QUINTA', 'Friday': 'SEXTA', 'Saturday': 'SÁBADO', 'Sunday': 'DOMINGO'
}


class BaseDataProcessor(ABC):
    """Classe base abstrata para processadores de dados com pipeline completo."""
    
    def __init__(self, base_dir: Path, ano: int = 2026, config: Optional[ProcessingConfig] = None):
        """
        Inicializa o processador base.
        
        Args:
            base_dir: Diretório base onde estão os arquivos de entrada
            ano: Ano para processamento
            config: Configuração de processamento (se None, usa DEFAULT_CONFIG)
        """
        self.base_dir = Path(base_dir)
        self.ano = ano
        self.config = config or DEFAULT_CONFIG
        self.metrics = SourceMetrics(nome_fonte=self.get_source_name())
        self.output_dir = self.base_dir / self.get_source_name() / f"resultados_{ano}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Log da configuração
        print(f"   📋 Modo de processamento: {self.config.get_description()}")
    
    @abstractmethod
    def get_source_name(self) -> str:
        """Retorna o nome da fonte de dados."""
        pass
    
    @abstractmethod
    def extrair_dados_historicos(self) -> pd.DataFrame:
        """Etapa 1: Extrai dados históricos da fonte."""
        pass
    
    @abstractmethod
    def criar_curva_tipica(self, df_historico: pd.DataFrame) -> pd.DataFrame:
        """Etapa 2: Cria curva típica a partir dos dados históricos."""
        pass
    
    @abstractmethod
    def gerar_projecao(self, curva_tipica: pd.DataFrame, mes: int) -> pd.DataFrame:
        """Etapa 3: Gera projeção para um mês específico."""
        pass
    
    def processar_pipeline_completo(self, meses: List[int]) -> Dict[str, pd.DataFrame]:
        """
        Executa pipeline completo: Extração → Curva Típica → Projeção.
        
        Args:
            meses: Lista de meses a processar
            
        Returns:
            Dicionário com DataFrames: {'historico', 'curva_tipica', 'projecoes'}
        """
        self.metrics.performance_metrics.iniciar()
        
        print(f"\n{'='*60}")
        print(f"PIPELINE COMPLETO - {self.get_source_name().upper()}")
        print(f"{'='*60}\n")
        
        # Verificar se deve reutilizar curva típica existente
        arquivo_curva = self.output_dir / f"curva_tipica_{self.get_source_name()}.csv"
        curva_existente = REUSAR_CURVAS_TIPICAS and arquivo_curva.exists()
        
        if curva_existente:
            print(f"♻️  REUTILIZANDO curva típica existente: {arquivo_curva.name}")
            try:
                curva_tipica = pd.read_csv(arquivo_curva, sep=';', decimal=',')
                self.metrics.data_metrics.dados_curva_tipica = len(curva_tipica)
                print(f"   ✅ {len(curva_tipica):,} registros carregados da curva típica")
                
                # Marcar que pulou extração e limpeza
                aviso = "Curva típica reutilizada - métricas de extração e limpeza não coletadas"
                self.metrics.avisos.append(aviso)
                
                df_historico = pd.DataFrame()  # Vazio pois não foi extraído
                
            except Exception as e:
                erro_msg = f"Erro ao carregar curva típica existente: {str(e)}. Recriando..."
                self.metrics.avisos.append(erro_msg)
                print(f"   ⚠️  {erro_msg}")
                curva_existente = False
        
        # Se não reutilizar, executar pipeline completo de criação
        if not curva_existente:
            # Etapa 1: Extração
            print("📥 ETAPA 1: Extraindo dados históricos...")
            try:
                df_historico = self.extrair_dados_historicos()
                self.metrics.data_metrics.dados_extraidos = len(df_historico)
                self.metrics.data_metrics.dados_brutos = getattr(self.metrics.data_metrics, 'dados_brutos', len(df_historico))
                print(f"   ✅ {len(df_historico):,} registros extraídos")
                self.metrics.performance_metrics.amostrar_memoria()
            except Exception as e:
                erro_msg = f"Erro na extração: {str(e)}"
                self.metrics.erros.append(erro_msg)
                print(f"   ❌ {erro_msg}")
                self.metrics.performance_metrics.finalizar(total_registros=0)
                return {}
            
            # Etapa 2: Curva Típica
            print("\n📊 ETAPA 2: Criando curva típica...")
            try:
                # Iniciar medição específica da curva típica
                self.metrics.performance_metrics.iniciar_curva_tipica()
                
                curva_tipica = self.criar_curva_tipica(df_historico)
                self.metrics.data_metrics.dados_curva_tipica = len(curva_tipica)
                self.metrics.data_metrics.pontos_curva_tipica = len(curva_tipica)
                
                # Finalizar medição da curva típica
                self.metrics.performance_metrics.finalizar_curva_tipica(pontos_gerados=len(curva_tipica))
                
                print(f"   ✅ {len(curva_tipica):,} registros na curva típica")
                print(f"   ⏱️  Tempo: {self.metrics.performance_metrics.tempo_curva_tipica:.2f}s")
                print(f"   💾 Memória: {self.metrics.performance_metrics.curva_tipica_memoria_delta_mb:+.2f} MB")
                
                # Salvar curva típica
                curva_tipica.to_csv(arquivo_curva, index=False, sep=';', decimal=',')
                self.metrics.arquivos_processados.append(str(arquivo_curva))
                print(f"   💾 Curva típica salva: {arquivo_curva.name}")
                self.metrics.performance_metrics.amostrar_memoria()
            except Exception as e:
                erro_msg = f"Erro na criação da curva típica: {str(e)}"
                self.metrics.erros.append(erro_msg)
                print(f"   ❌ {erro_msg}")
                curva_tipica = pd.DataFrame()
        
        # Etapa 3: Projeções (sempre executada)
        print("\n🔮 ETAPA 3: Gerando projeções...")
        projecoes = []
        for mes in meses:
            try:
                df_projecao = self.gerar_projecao(curva_tipica, mes)
                if not df_projecao.empty:
                    projecoes.append(df_projecao)
                    self.metrics.data_metrics.dados_projecao += len(df_projecao)
                    self.metrics.data_metrics.dados_projecao_por_mes[mes] = len(df_projecao)
                    
                    # Salvar projeção do mês
                    arquivo_projecao = self.output_dir / f"forecast_{self.get_source_name()}_{mes:02d}-{self.ano}.csv"
                    df_projecao.to_csv(arquivo_projecao, index=False, sep=';', decimal=',')
                    self.metrics.arquivos_processados.append(str(arquivo_projecao))
                    print(f"   ✅ Mês {mes:02d}: {len(df_projecao):,} registros → {arquivo_projecao.name}")
                
                self.metrics.performance_metrics.amostrar_memoria()
            except Exception as e:
                erro_msg = f"Erro na projeção do mês {mes}: {str(e)}"
                self.metrics.erros.append(erro_msg)
                print(f"   ❌ {erro_msg}")
        
        # Consolidar resultados
        df_projecoes_final = pd.concat(projecoes, ignore_index=True) if projecoes else pd.DataFrame()
        self.metrics.data_metrics.dados_finais = len(df_projecoes_final)
        
        # Finalizar métricas de performance
        # Throughput: 
        # - Se reutilizou curva típica: usar dados_finais (pois é o que foi processado)
        # - Se criou do zero: usar dados_extraidos/brutos (volume processado)
        if curva_existente:
            # Quando reutiliza, o trabalho é gerar a projeção a partir da curva
            total_processados = max(
                self.metrics.data_metrics.dados_curva_tipica,
                self.metrics.data_metrics.dados_finais
            )
        else:
            # Quando cria do zero, o trabalho é processar os dados brutos
            total_processados = max(
                self.metrics.data_metrics.dados_extraidos,
                self.metrics.data_metrics.dados_brutos
            )
        
        self.metrics.performance_metrics.finalizar(total_registros=total_processados)
        
        # Adicionar informações sobre o modo usado
        if curva_existente:
            self.metrics.versao_fonte = "Curva típica reutilizada"
        
        print(f"\n{'='*60}")
        print(f"PIPELINE CONCLUÍDO - {self.get_source_name().upper()}")
        print(f"   Tempo total: {self.metrics.performance_metrics.tempo_total_segundos:.2f}s")
        print(f"   Dados finais: {self.metrics.data_metrics.dados_finais:,} registros")
        if curva_existente:
            print(f"   ♻️  Curva típica reutilizada")
        print(f"{'='*60}\n")
        
        return {
            'historico': df_historico if not curva_existente else pd.DataFrame(),
            'curva_tipica': curva_tipica,
            'projecoes': df_projecoes_final
        }
    
    @staticmethod
    def remover_acentos(texto: str) -> str:
        """Remove acentos de uma string."""
        if pd.isna(texto):
            return texto
        texto_normalizado = unicodedata.normalize('NFD', str(texto))
        return ''.join(
            char for char in texto_normalizado 
            if unicodedata.category(char) != 'Mn'
        )


class CargaProcessor(BaseDataProcessor):
    """Processador de dados de Carga com pipeline completo."""
    
    def __init__(self, base_dir: Path, ano: int = 2026, config: Optional[ProcessingConfig] = None):
        super().__init__(base_dir, ano, config)
        self.mwmed_dict = None
        self.cadic_dict = None
    
    def get_source_name(self) -> str:
        return "carga"
    
    def _extrair_mwmed_cadic_do_newave(self):
        """Extrai MWmed e CAdic diretamente do NEWAVE (SISTEMA.DAT e C_ADIC.DAT)."""
        zip_path = self.base_dir / "deck_newave_2025_11.zip"
        
        if not zip_path.exists():
            aviso = f"Deck NEWAVE não encontrado: {zip_path}. Usando valores padrão."
            self.metrics.avisos.append(aviso)
            print(f"   ⚠️  {aviso}")
            return False
        
        try:
            # Parsear SISTEMA.DAT
            self.mwmed_dict = self._parse_sistema_dat_from_zip(str(zip_path), self.ano)
            print(f"   ✅ MWmed extraído do SISTEMA.DAT")
            
            # Parsear C_ADIC.DAT
            self.cadic_dict = self._parse_cadic_dat_from_zip(str(zip_path), self.ano)
            print(f"   ✅ CAdic extraído do C_ADIC.DAT")
            
            return True
        except Exception as e:
            erro_msg = f"Erro ao parsear deck NEWAVE: {str(e)}"
            self.metrics.erros.append(erro_msg)
            print(f"   ❌ {erro_msg}")
            return False
    
    def _parse_sistema_dat_from_zip(self, zip_path: str, ano: int) -> Dict[str, Dict[int, float]]:
        """Parseia SISTEMA.DAT do ZIP (lógica de parse_sistema_dat.py)."""
        import zipfile
        
        subsistema_map = {1: "SE", 2: "S", 3: "NE", 4: "N"}
        resultado = {sub: {} for sub in subsistema_map.values()}
        
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open("SISTEMA.DAT") as f:
                linhas = f.read().decode('latin-1', errors='ignore').splitlines()
        
        inicio_secao = False
        i = 0
        
        while i < len(linhas):
            linha = linhas[i].strip()
            
            if "MERCADO DE ENERGIA TOTAL" in linha.upper():
                inicio_secao = True
                i += 1
                continue
            
            if inicio_secao:
                if linha.upper() in ['XXX', ''] or 'XXXJAN' in linha.upper() or linha == "999":
                    if linha == "999":
                        break
                    i += 1
                    continue
                
                match_subsistema = re.match(r'^\s*(\d+)\s*$', linha)
                if match_subsistema:
                    subsistema_num = int(match_subsistema.group(1))
                    
                    if subsistema_num in subsistema_map:
                        subsistema = subsistema_map[subsistema_num]
                        i += 1
                        
                        while i < len(linhas):
                            linha_ano = linhas[i].strip()
                            
                            match_ano = re.match(rf'^\s*{ano}\s+', linha_ano)
                            if match_ano:
                                valores = re.findall(r'(\d+\.?\d*)', linha_ano)
                                
                                if len(valores) >= 13:  # Ano + 12 meses
                                    valores_meses = [float(v) for v in valores[1:13]]
                                    for mes_num, valor in enumerate(valores_meses, start=1):
                                        if valor > 0:
                                            resultado[subsistema][mes_num] = valor
                                
                                i += 1
                                break
                            
                            if linha_ano == "999" or linha_ano.startswith("POS"):
                                break
                            
                            i += 1
            
            i += 1
        
        return resultado
    
    def _parse_cadic_dat_from_zip(self, zip_path: str, ano: int) -> Dict[str, Dict[int, float]]:
        """Parseia C_ADIC.DAT do ZIP (lógica de parse_cadic_dat.py)."""
        import zipfile
        
        subsistema_map = {"SUDESTE": "SE", "SUL": "S", "NORDESTE": "NE", "NORTE": "N"}
        resultado = {sub: {mes: 0.0 for mes in range(1, 13)} for sub in subsistema_map.values()}
        
        with zipfile.ZipFile(zip_path, 'r') as z:
            with z.open("C_ADIC.DAT") as f:
                linhas = f.read().decode('latin-1', errors='ignore').splitlines()
        
        i = 0
        subsistema_atual = None
        
        while i < len(linhas):
            linha = linhas[i].strip()
            
            match_subsistema = re.match(r'^\s*(\d+)\s+(SUDESTE|SUL|NORDESTE|NORTE)', linha, re.IGNORECASE)
            
            if match_subsistema:
                nome_subsistema = match_subsistema.group(2).upper()
                if nome_subsistema in subsistema_map:
                    subsistema_atual = subsistema_map[nome_subsistema]
                    i += 1
                    
                    while i < len(linhas):
                        linha_ano = linhas[i].strip()
                        
                        match_ano = re.match(rf'^\s*{ano}\s+', linha_ano)
                        if match_ano:
                            valores = re.findall(r'(\d+\.?\d*)', linha_ano)
                            
                            if len(valores) >= 13:  # Ano + 12 meses
                                valores_meses = [float(v) for v in valores[1:13]]
                                for mes_num, valor in enumerate(valores_meses, start=1):
                                    if valor > 0:
                                        resultado[subsistema_atual][mes_num] += valor
                            
                            i += 1
                            continue
                        
                        if linha_ano.startswith("POS") or linha_ano == "999":
                            break
                        
                        i += 1
                    
                    continue
            
            i += 1
        
        return resultado
    
    def extrair_dados_historicos(self) -> pd.DataFrame:
        """Extrai dados históricos de carga SEMPRE baixando do ONS (lógica de script_ons.py)."""
        print(f"   📥 Baixando dados históricos de carga do ONS...")
        
        try:
            df = self._baixar_dados_ons()
            
            if df is None or df.empty:
                erro_msg = "Nenhum dado foi baixado do ONS"
                self.metrics.erros.append(erro_msg)
                print(f"   ❌ {erro_msg}")
                return pd.DataFrame()
            
            # Normalizar colunas
            df.columns = df.columns.str.lower()
            
            # Converter para datetime
            if 'din_instante' in df.columns:
                df['din_instante'] = pd.to_datetime(df['din_instante'])
            
            # Converter coluna de carga para numérico
            if 'val_cargaenergiahomwmed' in df.columns:
                df['val_cargaenergiahomwmed'] = pd.to_numeric(df['val_cargaenergiahomwmed'], errors='coerce')
            
            self.metrics.data_metrics.dados_brutos = len(df)
            print(f"   ✅ Total de registros extraídos: {len(df):,}")
            
            return df
            
        except Exception as e:
            erro_msg = f"Erro ao baixar dados do ONS: {str(e)}"
            self.metrics.erros.append(erro_msg)
            print(f"   ❌ {erro_msg}")
            return pd.DataFrame()
    
    def _baixar_dados_ons(self) -> pd.DataFrame:
        """Baixa dados históricos do ONS (lógica de script_ons.py)."""
        import io
        import requests
        from datetime import datetime
        
        ano_inicial = 2015
        ano_final = datetime.now().year
        url_base = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/curva-carga-ho/CURVA_CARGA_{}.csv"
        
        lista_dfs = []
        
        for ano in range(ano_inicial, ano_final + 1):
            url = url_base.format(ano)
            print(f"      Baixando ano {ano}...")
            
            try:
                response = requests.get(url, timeout=60)
                response.raise_for_status()
                
                arquivo_em_memoria = io.BytesIO(response.content)
                df_anual = pd.read_csv(arquivo_em_memoria, delimiter=';', decimal=',')
                lista_dfs.append(df_anual)
                
                print(f"      ✅ {len(df_anual):,} linhas baixadas")
            except Exception as e:
                print(f"      ⚠️  Erro no ano {ano}: {str(e)}")
        
        if not lista_dfs:
            return pd.DataFrame()
        
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        df_consolidado['val_cargaenergiahomwmed'] = pd.to_numeric(
            df_consolidado['val_cargaenergiahomwmed'], errors='coerce'
        )
        
        print(f"   ✅ Consolidação concluída: {len(df_consolidado):,} registros totais")
        
        return df_consolidado
    
    def criar_curva_tipica(self, df_historico: pd.DataFrame) -> pd.DataFrame:
        """Cria curva típica de carga (lógica de typical_curve.py)."""
        if df_historico.empty:
            return pd.DataFrame()
        
        # Pivotar por subsistema
        df_pivot = df_historico.pivot_table(
            index='din_instante',
            columns='id_subsistema',
            values='val_cargaenergiahomwmed',
            aggfunc='sum'
        ).reset_index()
        
        # Mapear IDs para nomes
        mapeamento = {1: "SE", 2: "S", 3: "NE", 4: "N"}
        df_pivot = df_pivot.rename(columns=mapeamento)
        
        # Adicionar colunas auxiliares
        df_pivot['Data'] = df_pivot['din_instante'].dt.floor('D')
        df_pivot['Ano'] = df_pivot['din_instante'].dt.year
        df_pivot['Mes'] = df_pivot['din_instante'].dt.month
        df_pivot['Tipo_Dia_Num'] = df_pivot['din_instante'].dt.weekday
        df_pivot['Hora'] = df_pivot['din_instante'].dt.hour
        
        # Calcular carga normalizada por mês (pu)
        for sub in SUBSISTEMAS:
            if sub in df_pivot.columns:
                media_mensal = df_pivot.groupby(['Ano', 'Mes'])[sub].transform('mean')
                df_pivot[f"{sub}_pu"] = (df_pivot[sub] / media_mensal).fillna(0)
        
        # Criar curva típica: média e desvio por mês, tipo de dia e hora
        colunas_pu = [f"{sub}_pu" for sub in SUBSISTEMAS if f"{sub}_pu" in df_pivot.columns]
        
        agg_dict = {col: ['mean', 'std'] for col in colunas_pu}
        curva_tipica = df_pivot.groupby(['Mes', 'Tipo_Dia_Num', 'Hora'])[colunas_pu].agg(agg_dict).reset_index()
        
        # Renomear colunas
        curva_tipica.columns = ['Mes', 'Tipo_Dia_Num', 'Hora'] + [
            f"{col}_{stat}" for col, stat in curva_tipica.columns[3:]
        ]
        
        self.metrics.data_metrics.dados_limpos = len(df_pivot)
        
        return curva_tipica
    
    def gerar_projecao(self, curva_tipica: pd.DataFrame, mes: int) -> pd.DataFrame:
        """Gera projeção de carga (lógica de generation_curve.py)."""
        if curva_tipica.empty:
            return pd.DataFrame()
        
        # Extrair MWmed e CAdic do NEWAVE (se ainda não foi feito)
        if self.mwmed_dict is None or self.cadic_dict is None:
            if not self._extrair_mwmed_cadic_do_newave():
                aviso = "Não foi possível extrair dados do NEWAVE"
                self.metrics.avisos.append(aviso)
                return pd.DataFrame()
        
        # Carregar calendário
        calendario_file = self.base_dir / "calendario_horario_2015_2030.xlsx"
        if not calendario_file.exists():
            # Tentar no diretório pai
            calendario_file = self.base_dir.parent / "calendario_horario_2015_2030.xlsx"
            if not calendario_file.exists():
                aviso = f"Calendário não encontrado"
                self.metrics.avisos.append(aviso)
                return pd.DataFrame()
        
        df_calendario = pd.read_excel(calendario_file)
        df_calendario['DataHora'] = pd.to_datetime(df_calendario['DataHora'])
        
        # Selecionar período de acordo com a configuração
        if self.config.mode == PeriodMode.SEMANA_TIPICA:
            # Modo original: semana típica
            df_periodo = self._encontrar_semana_tipica(df_calendario, mes, self.ano)
        else:
            # Modo período completo
            df_periodo = self._selecionar_periodo_completo(df_calendario, mes, self.ano)
        
        if df_periodo.empty:
            return pd.DataFrame()
        
        # Filtrar curva típica para o mês
        curva_mes = curva_tipica[curva_tipica['Mes'] == mes].copy()
        
        # Criar DataFrame base replicando a curva típica para todos os dias do período
        df_resultado = self._expandir_curva_para_periodo(curva_mes, df_periodo)
        
        # Garantir que tem coluna Mes
        if 'Mes' not in df_resultado.columns:
            df_resultado['Mes'] = mes
        
        # Aplicar projeção: (média_pu) * (MWmed + CAdic)
        for sub in SUBSISTEMAS:
            col_mean = f"{sub}_pu_mean"
            if col_mean in df_resultado.columns:
                if mes in self.mwmed_dict.get(sub, {}) and mes in self.cadic_dict.get(sub, {}):
                    mwmed = self.mwmed_dict[sub][mes]
                    cadic = self.cadic_dict[sub][mes]
                    df_resultado[sub] = df_resultado[col_mean] * (mwmed + cadic)
        
        # Reordenar colunas finais (já tem DataHora, Flag_Feriado, Patamar do merge anterior)
        colunas_carga = [col for col in df_resultado.columns if col in SUBSISTEMAS]
        colunas_ordenadas = ['Mes', 'Tipo_Dia_Num', 'Hora', 'DataHora', 'Flag_Feriado', 'Patamar'] + colunas_carga
        df_resultado = df_resultado[[col for col in colunas_ordenadas if col in df_resultado.columns]]
        
        return df_resultado
    
    def _encontrar_semana_tipica(self, df_calendario: pd.DataFrame, mes: int, ano: int) -> pd.DataFrame:
        """Encontra semana típica (lógica de generation_curve.py)."""
        # Converter Flag_Feriado para booleano
        df_calendario = df_calendario.copy()
        if df_calendario['Flag_Feriado'].dtype == 'object':
            df_calendario['Flag_Feriado'] = df_calendario['Flag_Feriado'].str.upper().str.strip() == 'VERDADEIRO'
        
        # Garantir coluna Data
        if 'Data' not in df_calendario.columns:
            if 'DataHora' in df_calendario.columns:
                df_calendario['Data'] = pd.to_datetime(df_calendario['DataHora']).dt.date
        
        df_calendario['Data'] = pd.to_datetime(df_calendario['Data'])
        
        # Filtrar para o mês/ano
        df_mes = df_calendario[
            (df_calendario['Data'].dt.month == mes) &
            (df_calendario['Data'].dt.year == ano)
        ].copy()
        
        if df_mes.empty:
            return pd.DataFrame()
        
        # Agrupar por data
        df_dias = df_mes.groupby('Data').agg({
            'Flag_Feriado': 'first',
            'DiaSemana_Num': 'first'
        }).reset_index().sort_values('Data').reset_index(drop=True)
        
        # Procurar semana completa começando em segunda (DiaSemana_Num = 1)
        for i in range(len(df_dias) - 6):
            semana = df_dias.iloc[i:i+7]
            if semana['DiaSemana_Num'].iloc[0] == 1:
                dias_semana = semana['DiaSemana_Num'].tolist()
                if dias_semana == [1, 2, 3, 4, 5, 6, 7]:
                    datas_semana = semana['Data'].tolist()
                    df_semana_tipica = df_mes[
                        pd.to_datetime(df_mes['Data']).dt.date.isin([d.date() for d in datas_semana])
                    ].copy()
                    return df_semana_tipica
        
        return pd.DataFrame()
    
    def _selecionar_periodo_completo(self, df_calendario: pd.DataFrame, mes: int, ano: int) -> pd.DataFrame:
        """
        Seleciona período completo (dias seguidos) de acordo com a configuração.
        
        Args:
            df_calendario: DataFrame do calendário
            mes: Mês a processar
            ano: Ano a processar
            
        Returns:
            DataFrame com o período selecionado
        """
        df_calendario = df_calendario.copy()
        
        # Garantir coluna Data
        if 'Data' not in df_calendario.columns:
            if 'DataHora' in df_calendario.columns:
                df_calendario['Data'] = pd.to_datetime(df_calendario['DataHora']).dt.date
        
        df_calendario['Data'] = pd.to_datetime(df_calendario['Data'])
        
        # Converter Flag_Feriado para booleano
        if df_calendario['Flag_Feriado'].dtype == 'object':
            df_calendario['Flag_Feriado'] = df_calendario['Flag_Feriado'].str.upper().str.strip() == 'VERDADEIRO'
        
        # Filtrar para o mês/ano
        df_mes = df_calendario[
            (df_calendario['Data'].dt.month == mes) &
            (df_calendario['Data'].dt.year == ano)
        ].copy()
        
        if df_mes.empty:
            return pd.DataFrame()
        
        # Se dias_por_mes é None, usar o mês completo
        if self.config.dias_por_mes is None:
            return df_mes
        
        # Caso contrário, limitar ao número de dias especificado
        datas_unicas = sorted(df_mes['Data'].unique())
        dias_para_processar = min(self.config.dias_por_mes, len(datas_unicas))
        datas_selecionadas = datas_unicas[:dias_para_processar]
        
        df_periodo = df_mes[df_mes['Data'].isin(datas_selecionadas)].copy()
        
        return df_periodo
    
    def _expandir_curva_para_periodo(self, curva_mes: pd.DataFrame, df_periodo: pd.DataFrame) -> pd.DataFrame:
        """
        Expande a curva típica para cobrir todos os dias do período selecionado.
        
        Args:
            curva_mes: DataFrame com a curva típica do mês (com Mes, Tipo_Dia_Num, Hora)
            df_periodo: DataFrame do calendário com os dias a processar
            
        Returns:
            DataFrame expandido com valores para cada hora de cada dia
        """
        # Preparar dados do período
        df_ref = df_periodo.copy()
        
        # Converter DiaSemana_Num do Excel (1-7) para Python (0-6)
        if 'DiaSemana_Num' in df_ref.columns:
            df_ref['Tipo_Dia_Num'] = (df_ref['DiaSemana_Num'] - 1) % 7
        
        # Garantir coluna Hora
        if 'Hora' not in df_ref.columns and 'DataHora' in df_ref.columns:
            df_ref['Hora'] = pd.to_datetime(df_ref['DataHora']).dt.hour
        
        # Criar chave de merge única para cada dia
        if 'Data' not in df_ref.columns and 'DataHora' in df_ref.columns:
            df_ref['Data'] = pd.to_datetime(df_ref['DataHora']).dt.date
        
        df_ref['Data'] = pd.to_datetime(df_ref['Data'])
        df_ref['DiaSeq'] = (df_ref['Data'] - df_ref['Data'].min()).dt.days
        
        # Merge curva típica com cada combinação de (Data, Tipo_Dia_Num, Hora)
        df_periodo_keys = df_ref[['Data', 'DiaSeq', 'Tipo_Dia_Num', 'Hora', 'DataHora', 'Flag_Feriado', 'Patamar']].drop_duplicates()
        
        # Fazer merge da curva típica com todas as horas do período
        df_resultado = pd.merge(
            df_periodo_keys,
            curva_mes,
            on=['Tipo_Dia_Num', 'Hora'],
            how='left'
        )
        
        # Ordenar por Data e Hora
        df_resultado = df_resultado.sort_values(['Data', 'Hora']).reset_index(drop=True)
        
        return df_resultado
    
    def _adicionar_colunas_calendario(self, df_carga: pd.DataFrame, df_semana_tipica: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas do calendário ao DataFrame."""
        df_resultado = df_carga.copy()
        
        df_ref = df_semana_tipica.copy()
        
        # Converter DiaSemana_Num do Excel (1-7) para Python (0-6)
        if 'DiaSemana_Num' in df_ref.columns:
            df_ref['Tipo_Dia_Num'] = (df_ref['DiaSemana_Num'] - 1) % 7
        
        # Merge
        df_merge = df_ref[['Tipo_Dia_Num', 'Hora', 'DataHora', 'Flag_Feriado', 'Patamar']].drop_duplicates(
            subset=['Tipo_Dia_Num', 'Hora']
        )
        
        df_resultado = df_resultado.merge(df_merge, on=['Tipo_Dia_Num', 'Hora'], how='left')
        
        # Reordenar colunas
        colunas_carga = [col for col in df_resultado.columns if col in SUBSISTEMAS]
        colunas_ordenadas = ['Mes', 'Tipo_Dia_Num', 'Hora', 'DataHora', 'Flag_Feriado', 'Patamar'] + colunas_carga
        df_resultado = df_resultado[[col for col in colunas_ordenadas if col in df_resultado.columns]]
        
        return df_resultado


class EolUfvMmgdProcessor(BaseDataProcessor):
    """Processador de EOL/UFV/MMGD com pipeline completo usando Snowflake."""
    
    def __init__(self, base_dir: Path, ano: int = 2026, config: Optional[ProcessingConfig] = None):
        super().__init__(base_dir, ano, config)
        self.snowflake_conn = None
        self.installed_capacity = None
        self.boost = False
        self.available_percentiles = [10, 25, 50, 75, 90]
    
    def get_source_name(self) -> str:
        return "eol_ufv_mmgd"
    
    def _connect_snowflake(self):
        """Conecta ao Snowflake usando credenciais do AWS SSM."""
        try:
            sys.path.insert(0, str(self.base_dir / "utils"))
            from utils import utils_aws, utils_snowflake
            
            print("🔐 Conectando ao Snowflake...")
            SystemsManager = utils_aws.SSM()
            snowflake_credentials = SystemsManager.get_parameter(
                name='/snowflake/automationservice/rsa',
                decrypted=False
            )
            self.snowflake_conn = utils_snowflake.SnowflakeSession(
                snowflake_credentials=snowflake_credentials
            )
            self.snowflake_conn.connect()
            print("✅ Conexão com Snowflake estabelecida")
            return True
        except Exception as e:
            aviso = f"Erro ao conectar ao Snowflake: {e}"
            self.metrics.avisos.append(aviso)
            print(f"❌ {aviso}")
            return False
    
    def _get_typical_year(self, month: int) -> pd.DataFrame:
        """
        Busca dados de ano típico do Snowflake.
        Implementa get_typical_year() do generation_curve.py
        """
        eol_source = 'EOL_V2'
        ufv_source = 'UFV'
        mmgd_source = 'GD_AVG'
        
        query = f"""
                SELECT
                    CASE 
                        WHEN ENERGY_SOURCE = '{eol_source}' THEN 'EOL'
                        WHEN ENERGY_SOURCE = '{ufv_source}' THEN 'UFV'
                        WHEN ENERGY_SOURCE = '{mmgd_source}' THEN 'MMGD'
                        ELSE ENERGY_SOURCE
                    END AS ENERGY_SOURCE,
                    ID_SUBMARKET,
                    METRIC_NAME,
                    HOUROFDAY,
                    VALUE AS FC
                FROM
                    OEM_DEV.ONS.F_REAL_SUBMARKET_CAPACITYFACTOR_HOURLY_MONTHLY
                WHERE
                    MONTHOFYEAR = {month}
                    AND ENERGY_SOURCE IN ('{eol_source}', '{ufv_source}', '{mmgd_source}')
                    AND METRIC_NAME != 'STD'
                ORDER BY ENERGY_SOURCE, ID_SUBMARKET, METRIC_NAME, HOUROFDAY
                """
        
        response = self.snowflake_conn.query_to_dataframe(query)
        print(f"  ✓ Ano típico mês {month}: {len(response)} registros")
        return response
    
    def _get_installed_capacity(self) -> pd.DataFrame:
        """
        Busca capacidade instalada do Snowflake.
        Implementa get_installed_capacity() do generation_curve.py
        """
        basis_condition = "WHERE SCENARIO = 'Dez+'"
        
        query = f"""
                SELECT
                    ENERGY_SOURCE,
                    ID_SUBMARKET,
                    VALUE AS INSTALLED_CAPACITY,
                    GENERATION_PERCENTILE
                FROM OEM_DEV.ONS.F_REAL_SUBMARKET_INSTALLEDCAPACITY
                {basis_condition}
                ORDER BY ENERGY_SOURCE, ID_SUBMARKET
                """
        
        response = self.snowflake_conn.query_to_dataframe(query)
        print(f"  ✓ Capacidade instalada: {len(response)} registros")
        return response
    
    def _estimate_percentile(self, percentile: float, known_values: list) -> float:
        """Interpola valores para percentis arbitrários."""
        return np.interp(percentile, self.available_percentiles, known_values)
    
    def _process_percentile(self, fonte: str, submercado: str, percentil: float, df: pd.DataFrame) -> list:
        """
        Processa percentis customizados usando interpolação.
        Implementa a classe Percentile do generation_curve.py
        """
        query = f"ENERGY_SOURCE == '{fonte}' & ID_SUBMARKET == '{submercado}'"
        values_dict = {}
        
        for p in self.available_percentiles:
            values_dict[p] = df.query(f"{query} & METRIC_NAME == 'P{p}'")["POWER"].values
        
        # Interpolar cada hora separadamente
        estimated = []
        for i in range(24):
            known_vals = [values_dict[p][i] for p in self.available_percentiles]
            est = self._estimate_percentile(percentil, known_vals)
            estimated.append(est)
        
        week_values = np.tile(estimated, 7)
        return week_values.tolist()
    
    def extrair_dados_historicos(self) -> pd.DataFrame:
        """
        Extrai dados históricos do Snowflake.
        Para EOL/UFV/MMGD, isso significa buscar capacidade instalada e ano típico.
        """
        if not self._connect_snowflake():
            return pd.DataFrame()
        
        print("📊 Extraindo dados de EOL/UFV/MMGD do Snowflake...")
        
        try:
            # Buscar capacidade instalada
            self.installed_capacity = self._get_installed_capacity()
            
            if self.installed_capacity.empty:
                aviso = "Capacidade instalada vazia"
                self.metrics.avisos.append(aviso)
                return pd.DataFrame()
            
            self.metrics.data_metrics.dados_brutos = len(self.installed_capacity)
            print(f"✅ Capacidade instalada extraída: {len(self.installed_capacity)} registros")
            
            return self.installed_capacity
        
        except Exception as e:
            aviso = f"Erro ao extrair dados do Snowflake: {e}"
            self.metrics.avisos.append(aviso)
            print(f"❌ {aviso}")
            return pd.DataFrame()
    
    def criar_curva_tipica(self, df_historico: pd.DataFrame) -> pd.DataFrame:
        """
        EOL/UFV/MMGD não usa curva típica tradicional.
        Os dados já vêm processados do Snowflake.
        """
        if df_historico.empty:
            return pd.DataFrame()
        
        # Preparar cenários
        cenarios = df_historico[["ENERGY_SOURCE", "ID_SUBMARKET", "GENERATION_PERCENTILE"]].copy()
        cenarios.rename({
            "ENERGY_SOURCE": "FONTE",
            "ID_SUBMARKET": "SUBMERCADO",
            "GENERATION_PERCENTILE": "PERCENTIL"
        }, axis=1, inplace=True)
        
        cenarios["PERCENTIL"] = [float(x[1:]) if x else 50 for x in cenarios["PERCENTIL"]]
        cenarios = cenarios[cenarios["FONTE"].isin(["EOL", "MMGD", "UFV"])].reset_index(drop=True)
        
        self.metrics.data_metrics.dados_limpos = len(cenarios)
        print(f"✅ Cenários preparados: {len(cenarios)}")
        
        return cenarios
    
    def gerar_projecao(self, curva_tipica: pd.DataFrame, mes: int) -> pd.DataFrame:
        """
        Gera projeção usando dados do Snowflake.
        Implementa a lógica completa do generation_curve.py
        """
        # Se installed_capacity é None, buscar do Snowflake (caso de reutilização de curva típica)
        if self.installed_capacity is None:
            if self._connect_snowflake():
                self.installed_capacity = self._get_installed_capacity()
        
        if curva_tipica.empty or self.installed_capacity is None:
            return pd.DataFrame()
        
        print(f"📈 Gerando projeção EOL/UFV/MMGD para mês {mes}...")
        
        try:
            # Buscar ano típico para o mês
            typical_year = self._get_typical_year(mes)
            
            if typical_year.empty:
                aviso = f"Ano típico vazio para mês {mes}"
                self.metrics.avisos.append(aviso)
                return pd.DataFrame()
            
            # Merge typical_year com installed_capacity
            merged = pd.merge(
                typical_year,
                self.installed_capacity,
                on=["ENERGY_SOURCE", "ID_SUBMARKET"],
                how="inner"
            )
            merged["VALOR"] = merged["FC"] * merged["INSTALLED_CAPACITY"]
            
            print(f"  Dados mesclados: {len(merged)} registros")
            
            # Preparar resultados
            resultados = []
            cenarios = curva_tipica  # cenarios já preparados em criar_curva_tipica
            
            for _, row in cenarios.iterrows():
                fonte = row['FONTE']
                submercado = row['SUBMERCADO']
                percentil = row['PERCENTIL']
                
                # Ajuste percentil se boost
                if self.boost:
                    if percentil == 50:
                        continue
                    elif percentil < 50:
                        percentil -= 15
                    else:
                        percentil += 15
                    percentil = np.clip(percentil, 1, 99)
                
                # Gera vetor de 168 horas (7 dias x 24h)
                if (fonte == "UFV") and ((submercado == "S") or (submercado == "N")):
                    week_values = [0.0] * 168
                elif (fonte == "EOL") and (submercado == "SE"):
                    week_values = self._process_percentile(
                        fonte, "S", percentil,
                        merged.rename(columns={"VALOR": "POWER"})
                    )
                    week_values = [x * 0.004 for x in week_values]
                else:
                    if percentil in self.available_percentiles:
                        hour_values = merged.query(
                            f"ENERGY_SOURCE == '{fonte}' & ID_SUBMARKET == '{submercado}' & METRIC_NAME == 'P{int(percentil)}'"
                        )["VALOR"].values
                        week_values = np.tile(hour_values, 7).tolist()
                    else:
                        week_values = self._process_percentile(
                            fonte, submercado, percentil,
                            merged.rename(columns={"VALOR": "POWER"})
                        )
                
                # Monta registros para cada hora da semana típica
                for week_idx, value in enumerate(week_values):
                    dia_semana = week_idx // 24
                    hora = week_idx % 24
                    resultados.append({
                        "MES": mes,
                        "DIA_SEMANA": dia_semana,
                        "HORA": hora,
                        "FONTE": fonte,
                        "SUBMERCADO": submercado,
                        "PERCENTIL": percentil,
                        "VALOR": value
                    })
            
            resultados_df = pd.DataFrame(resultados)
            
            # Salvar CSV para cache (opcional)
            output_dir = self.base_dir / "eol_uvf_mmgd" / "resultados_2026"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"forecast_ufv_mmgd_eol_{mes:02d}_2026.csv"
            resultados_df.to_csv(output_file, sep=";", index=False)
            
            print(f"✅ Projeção gerada: {len(resultados_df)} registros")
            print(f"💾 Salvo em: {output_file}")
            
            return resultados_df
        
        except Exception as e:
            aviso = f"Erro ao gerar projeção EOL/UFV/MMGD: {e}"
            self.metrics.avisos.append(aviso)
            print(f"❌ {aviso}")
            return pd.DataFrame()


class PchPctProcessor(BaseDataProcessor):
    """Processador de PCH/PCT com pipeline completo."""
    
    def __init__(self, base_dir: Path, ano: int = 2026, config: Optional[ProcessingConfig] = None):
        super().__init__(base_dir, ano, config)
        self.mwmed_dict = {}  # Será preenchido dinamicamente
    
    def get_source_name(self) -> str:
        return "pch_pct"
    
    def _parse_pch_pct_sistema_dat_from_zip(self, ano: int = 2026) -> Tuple[Dict[str, Dict[int, float]], Dict[str, Dict[int, float]]]:
        """
        Extrai valores de PCH e PCT do SISTEMA.DAT dentro do ZIP NEWAVE.
        Implementa a lógica de parse_pch_pct_sistema_dat.py
        """
        zip_path = self.base_dir / "deck_newave_2025_11.zip"
        
        if not zip_path.exists():
            aviso = f"Arquivo NEWAVE não encontrado: {zip_path}"
            self.metrics.avisos.append(aviso)
            return ({}, {})
        
        # Mapeamento de subsistemas
        subsistema_map = {1: "SE", 2: "S", 3: "NE", 4: "N"}
        
        # Inicializar dicionários de resultados
        pch_dict = {sub: {} for sub in subsistema_map.values()}
        pct_dict = {sub: {} for sub in subsistema_map.values()}
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open("SISTEMA.DAT") as f:
                    linhas = [linha.decode('latin-1', errors='ignore') for linha in f.readlines()]
            
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
                    
                    # Procurar linha com subsistema e tipo
                    linha_original = linhas[i]
                    match_entrada = re.match(r'^\s+(\d+)\s+(\d+)\s+(\w+)', linha_original)
                    if match_entrada:
                        subsistema_num = int(match_entrada.group(1))
                        tipo_num = int(match_entrada.group(2))
                        
                        # Só processar PCH (tipo 1) e PCT (tipo 2)
                        if tipo_num not in [1, 2] or subsistema_num not in subsistema_map:
                            i += 1
                            continue
                        
                        subsistema = subsistema_map[subsistema_num]
                        dict_alvo = pch_dict if tipo_num == 1 else pct_dict
                        i += 1
                        
                        # Procurar linha com o ano desejado
                        anos_procurar = [ano, ano + 1]
                        
                        while i < len(linhas):
                            linha_ano_raw = linhas[i]
                            linha_ano = linha_ano_raw.strip()
                            
                            # Verificar se é próxima entrada diferente
                            match_prox_entrada = re.match(r'^\s+(\d+)\s+(\d+)\s+\w+', linha_ano_raw)
                            if match_prox_entrada:
                                subsistema_prox = int(match_prox_entrada.group(1))
                                tipo_prox = int(match_prox_entrada.group(2))
                                if subsistema_prox != subsistema_num or tipo_prox != tipo_num:
                                    i -= 1
                                    break
                            
                            # Verificar se é linha de algum dos anos desejados
                            match_ano = None
                            ano_encontrado = None
                            for ano_proc in anos_procurar:
                                match_temp = re.match(rf'^\s*{ano_proc}\s+', linha_ano_raw)
                                if match_temp:
                                    match_ano = match_temp
                                    ano_encontrado = ano_proc
                                    break
                            
                            if match_ano:
                                valores = re.findall(r'(\d+\.?\d*)', linha_ano_raw)
                                
                                if len(valores) >= 1:
                                    valores_meses = valores[1:]  # Pular o primeiro valor (ano)
                                    valores_float = []
                                    for val in valores_meses:
                                        try:
                                            valores_float.append(float(val))
                                        except ValueError:
                                            valores_float.append(0.0)
                                    
                                    # Se tiver menos de 12 valores, preencher com zeros à esquerda
                                    if len(valores_float) < 12:
                                        valores_completos = [0.0] * 12
                                        inicio = 12 - len(valores_float)
                                        for j, val in enumerate(valores_float):
                                            valores_completos[inicio + j] = val
                                        valores_float = valores_completos
                                    elif len(valores_float) >= 12:
                                        valores_float = valores_float[:12]
                                    
                                    # Mapear meses
                                    for mes_destino in range(1, 13):
                                        idx = mes_destino - 1
                                        if idx < len(valores_float):
                                            valor = valores_float[idx]
                                            if valor > 0:
                                                if ano_encontrado == ano:
                                                    dict_alvo[subsistema][mes_destino] = valor
                                                elif ano_encontrado == ano + 1:
                                                    if mes_destino not in dict_alvo[subsistema] or dict_alvo[subsistema].get(mes_destino, 0) == 0:
                                                        dict_alvo[subsistema][mes_destino] = valor
                                
                                i += 1
                                continue
                            
                            if linha_ano == "999":
                                inicio_secao = False
                                break
                            
                            if linha_ano.startswith("POS"):
                                i += 1
                                continue
                            
                            i += 1
                            
                            if i >= len(linhas):
                                break
                        
                        i += 1
                        continue
                    
                    i += 1
                    continue
                
                i += 1
        
        except Exception as e:
            aviso = f"Erro ao parsear SISTEMA.DAT: {e}"
            self.metrics.avisos.append(aviso)
        
        return pch_dict, pct_dict
    
    def _baixar_dados_ons(self) -> pd.DataFrame:
        """
        Baixa dados históricos de PCH/PCT do ONS.
        Implementa a lógica de script_ons_pequenas_usinas.py
        """
        import io
        import requests
        from datetime import datetime
        
        ano_inicial = 2018
        ano_final = datetime.now().year
        mes_final = datetime.now().month
        
        url_anual = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{:04d}.csv"
        url_mensal = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{:04d}_{:02d}.csv"
        
        lista_dataframes = []
        
        print(f"🔽 Baixando dados históricos PCH/PCT do ONS ({ano_inicial}-{ano_final})...")
        
        for ano in range(ano_inicial, ano_final + 1):
            ultimo_mes = 12 if ano < ano_final else mes_final
            
            # Tentar arquivo anual primeiro
            url_do_arquivo_anual = url_anual.format(ano)
            
            try:
                response = requests.get(url_do_arquivo_anual, timeout=60)
                response.raise_for_status()
                
                arquivo_em_memoria = io.BytesIO(response.content)
                df_anual = pd.read_csv(arquivo_em_memoria, sep=';', decimal=',')
                lista_dataframes.append(df_anual)
                print(f"  ✓ {ano} - arquivo anual ({len(df_anual):,} linhas)")
            
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # Tentar arquivos mensais
                    print(f"  → {ano} - tentando arquivos mensais...")
                    for mes in range(1, ultimo_mes + 1):
                        url_do_arquivo = url_mensal.format(ano, mes)
                        try:
                            response = requests.get(url_do_arquivo, timeout=60)
                            response.raise_for_status()
                            
                            arquivo_em_memoria = io.BytesIO(response.content)
                            df_mensal = pd.read_csv(arquivo_em_memoria, sep=';', decimal=',')
                            lista_dataframes.append(df_mensal)
                            print(f"    ✓ {ano}-{mes:02d} ({len(df_mensal):,} linhas)")
                        except:
                            print(f"    ✗ {ano}-{mes:02d} não encontrado")
            except Exception as e:
                print(f"  ✗ {ano} - Erro: {e}")
        
        if not lista_dataframes:
            return pd.DataFrame()
        
        df_consolidado = pd.concat(lista_dataframes, ignore_index=True)
        
        # Converter tipos
        if 'din_instante' in df_consolidado.columns:
            df_consolidado['din_instante'] = pd.to_datetime(df_consolidado['din_instante'], errors='coerce')
        
        if 'val_geracao' in df_consolidado.columns:
            df_consolidado['val_geracao'] = pd.to_numeric(df_consolidado['val_geracao'], errors='coerce')
        
        print(f"✅ Total consolidado: {len(df_consolidado):,} linhas")
        
        return df_consolidado
   
    def extrair_dados_historicos(self) -> pd.DataFrame:
        """
        Extrai dados históricos de PCH/PCT.
        Primeiro extrai MWmed do NEWAVE, depois SEMPRE baixa dados históricos do ONS.
        """
        # 1. Extrair MWmed e MWmed_PCT do NEWAVE
        print("📊 Extraindo MWmed de PCH/PCT do SISTEMA.DAT...")
        pch_dict, pct_dict = self._parse_pch_pct_sistema_dat_from_zip(ano=2026)
        
        # Armazenar para uso posterior
        self.mwmed_dict = {"PCH": pch_dict, "PCT": pct_dict}
        
        # 2. SEMPRE baixar dados históricos do ONS (não reutilizar arquivos locais)
        print(f"📥 Baixando dados históricos do ONS...")
        df = self._baixar_dados_ons()
        
        if df.empty:
            aviso = "Não foi possível baixar dados do ONS"
            self.metrics.avisos.append(aviso)
            return pd.DataFrame()
        
        # Aplicar filtro de modalidade (excluir TIPO I e TIPO II-A)
        col_modalidade = None
        for col in df.columns:
            if 'modalidade' in col.lower() or 'cod_modalidade' in col.lower():
                col_modalidade = col
                break
        
        if col_modalidade:
            print(f"🔍 Aplicando filtro de modalidade (excluir TIPO I e TIPO II-A)...")
            antes = len(df)
            df = df[~df[col_modalidade].isin(['TIPO I', 'TIPO II-A'])]
            print(f"   Registros: {antes:,} → {len(df):,}")
        
        self.metrics.data_metrics.dados_brutos = len(df)
        return df
   
    def criar_curva_tipica(self, df_historico: pd.DataFrame) -> pd.DataFrame:
        """
        Cria curva típica para PCH e PCT.
        Implementa a lógica de typical_curve_pch_pct.py
        """
        if df_historico.empty:
            return pd.DataFrame()
        
        print("📈 Criando curva típica para PCH e PCT...")
        
        df_final_list = []
        
        # Processar cada tipo de geração
        for fonte, tipo_geracao in [("HIDROELÉTRICA", "PCH"), ("TÉRMICA", "PCT")]:
            df_fonte = df_historico[df_historico["nom_tipousina"] == fonte].copy()
            
            if df_fonte.empty:
                print(f"  ⚠️  Sem dados para {fonte}")
                continue
            
            # Pivotar por subsistema
            df_pivot = df_fonte.pivot_table(
                index='din_instante',
                columns='id_subsistema',
                values='val_geracao',
                aggfunc='sum'
            ).reset_index()
            
            # Mapear IDs
            mapeamento = {1: "SE", 2: "S", 3: "NE", 4: "N"}
            df_pivot = df_pivot.rename(columns=mapeamento)
            
            # Garantir colunas
            for sub in SUBSISTEMAS:
                if sub not in df_pivot.columns:
                    df_pivot[sub] = 0
            
            # Calcular SIN
            df_pivot['SIN'] = df_pivot[SUBSISTEMAS].sum(axis=1, skipna=True)
            
            # Colunas auxiliares
            df_pivot['Data'] = df_pivot['din_instante'].dt.floor('D')
            df_pivot['Ano'] = df_pivot['din_instante'].dt.year
            df_pivot['Mes'] = df_pivot['din_instante'].dt.month
            df_pivot['Dia'] = df_pivot['din_instante'].dt.day
            df_pivot['Tipo_Dia'] = df_pivot['din_instante'].dt.strftime("%a")
            df_pivot['Tipo_Dia_Num'] = df_pivot['din_instante'].dt.weekday
            df_pivot['Hora'] = df_pivot['din_instante'].dt.hour
            df_pivot['TIPO_GERACAO'] = tipo_geracao
            
            # Normalizar por média mensal
            for col in ["N", "NE", "S", "SE", "SIN"]:
                df_pivot[f"{col}_pu"] = df_pivot[col] / df_pivot.groupby(["Ano", "Mes"])[col].transform("mean")
            
            df_pivot = df_pivot.fillna(0)
            
            # Calcular média mensal
            mwmed_mensal = (
                df_pivot.groupby(["Ano", "Mes"])[["N", "NE", "S", "SE", "SIN"]]
                .mean()
                .reset_index()
                .rename_axis(None, axis=1)
            )
            
            # Merge completo
            df_completo = pd.merge(
                df_pivot,
                mwmed_mensal,
                on=["Ano", "Mes"],
                suffixes=("", "_MWmed")
            )
            
            df_final_list.append(df_completo)
            print(f"  ✓ {tipo_geracao}: {len(df_completo):,} registros")
        
        if not df_final_list:
            return pd.DataFrame()
        
        df_final = pd.concat(df_final_list, ignore_index=True)
        
        # Criar curvas típicas por tipo de geração
        curvas_tipicas = []
        
        for fonte in ["PCH", "PCT"]:
            df_pu = df_final[df_final["TIPO_GERACAO"] == fonte].copy()
            
            if df_pu.empty:
                continue
            
            # Criar curva típica
            curva_tipica = (
                df_pu
                .groupby(["Mes", "Tipo_Dia_Num", "Hora"])[["N_pu", "NE_pu", "S_pu", "SE_pu", "SIN_pu"]]
                .agg(["mean", "std"])
            )
            
            curva_tipica.columns = [f"{col}_{stat}" for col, stat in curva_tipica.columns]
            curva_tipica = curva_tipica.reset_index()
            curva_tipica['TIPO_GERACAO'] = fonte
            
            curvas_tipicas.append(curva_tipica)
            print(f"  ✓ Curva típica {fonte}: {len(curva_tipica):,} pontos")
        
        if curvas_tipicas:
            resultado = pd.concat(curvas_tipicas, ignore_index=True)
            self.metrics.data_metrics.dados_limpos = len(resultado)
            return resultado
        else:
            return pd.DataFrame()
  
    def gerar_projecao(self, curva_tipica: pd.DataFrame, mes: int) -> pd.DataFrame:
        """
        Gera projeção para PCH e PCT.
        Implementa a lógica de generation_curve_pch_pct.py
        """
        # Se mwmed_dict estiver vazio, extrair do NEWAVE (caso de reutilização de curva típica)
        if not self.mwmed_dict:
            pch_dict, pct_dict = self._parse_pch_pct_sistema_dat_from_zip(self.ano)
            self.mwmed_dict = {"PCH": pch_dict, "PCT": pct_dict}
        
        if curva_tipica.empty or not self.mwmed_dict:
            return pd.DataFrame()
        
        resultados = []
        
        for tipo in ["PCH", "PCT"]:
            curva_tipo = curva_tipica[curva_tipica['TIPO_GERACAO'] == tipo].copy()
            curva_mes = curva_tipo[curva_tipo['Mes'] == mes].copy()
            
            if curva_mes.empty:
                continue
            
            # Criar DataFrame base
            df_base = curva_mes[['Mes', 'Tipo_Dia_Num', 'Hora']].copy().reset_index(drop=True)
            
            # Aplicar projeção: (média - std) * MWmed
            for sub in SUBSISTEMAS:
                col_mean = f"{sub}_pu_mean"
                # col_std = f"{sub}_pu_std"
                
                if col_mean in curva_mes.columns:
                    # Usar MWmed do dicionário extraído do NEWAVE
                    if sub in self.mwmed_dict[tipo] and mes in self.mwmed_dict[tipo][sub]:
                        mwmed = self.mwmed_dict[tipo][sub][mes]
                        mean_vals = curva_mes[col_mean].reset_index(drop=True)
                        # std_vals = curva_mes[col_std].reset_index(drop=True)
                        df_base[f"{tipo} - {sub}"] = mean_vals * mwmed
                    else:
                        df_base[f"{tipo} - {sub}"] = 0.0
            
            resultados.append(df_base)
        
        if resultados:
            # Merge PCH e PCT
            if len(resultados) == 2:
                df_final = resultados[0].merge(resultados[1], on=['Mes', 'Tipo_Dia_Num', 'Hora'], how='outer')
            else:
                df_final = resultados[0]
            
            return df_final
        else:
            return pd.DataFrame()


class TermicaProcessor(BaseDataProcessor):
    """Processador de Térmica com pipeline completo."""
    
    def get_source_name(self) -> str:
        return "termica"
    
    def _parse_conft_dat(self, zfile: zipfile.ZipFile) -> pd.DataFrame:
        """
        Função auxiliar para ler o CONFT.DAT a partir de um objeto ZipFile aberto.
        Retorna um DataFrame com 'ID' e 'SSIS'.
        """
        all_files_in_zip = zfile.namelist()
        conft_dat_files = [f for f in all_files_in_zip if os.path.basename(f).upper() == "CONFT.DAT"]
        
        if not conft_dat_files:
            return pd.DataFrame(columns=['ID', 'SSIS'])
        
        conft_file_name = conft_dat_files[0]
        print(f"✅ 'CONFT.DAT' encontrado: '{conft_file_name}'")
        
        lines = zfile.open(conft_file_name).read().decode("latin1").splitlines()
        conft_registros = []
        conft_regex = re.compile(r"^\s*(\d+)\s+(.+?)\s+(\d+)\s+([A-Z]{2})\s+(\d+)\s*$")
        
        for i, line in enumerate(lines):
            if i < 2 or not line.strip():
                continue
            
            match = conft_regex.match(line)
            if not match:
                continue
            
            try:
                usina_id = int(match.group(1))
                ssis = int(match.group(3))
                conft_registros.append({"ID": usina_id, "SSIS": ssis})
            except ValueError:
                continue
        
        return pd.DataFrame(conft_registros)
    
    def _parse_clast_dat(self, zfile: zipfile.ZipFile) -> pd.DataFrame:
        """
        Função auxiliar para ler o CLAST.DAT a partir de um objeto ZipFile aberto.
        Retorna um DataFrame com 'ID', 'TIPO_COMBUSTIVEL', 'CUSTO_CVU'.
        """
        all_files_in_zip = zfile.namelist()
        clast_dat_files = [f for f in all_files_in_zip if os.path.basename(f).upper() == "CLAST.DAT"]
        
        if not clast_dat_files:
            return pd.DataFrame(columns=['ID', 'TIPO_COMBUSTIVEL', 'CUSTO_CVU'])
        
        clast_file_name = clast_dat_files[0]
        print(f"✅ 'CLAST.DAT' encontrado: '{clast_file_name}'")
        
        lines = zfile.open(clast_file_name).read().decode("latin1").splitlines()
        clast_registros = []
        clast_regex = re.compile(r"^\s*(\d+)\s+(.+?)\s+(.+?)\s+(?:[\d\.]+)\s+([\d\.]+)(?:\s+[\d\.]+)*\s*$")
        
        for i in range(2, len(lines)):
            line = lines[i]
            if line.strip() == "9999":
                break
            if not line.strip():
                continue
            
            match = clast_regex.match(line)
            if not match:
                continue
            
            try:
                usina_id = int(match.group(1))
                tipo_combustivel = match.group(3).strip()
                custo_cvu = float(match.group(4).replace(",", "."))
                
                clast_registros.append({
                    "ID": usina_id,
                    "TIPO_COMBUSTIVEL": tipo_combustivel,
                    "CUSTO_CVU": custo_cvu
                })
            except ValueError:
                continue
        
        return pd.DataFrame(clast_registros)
    
    def _parse_gtmin_excel(self, zfile: zipfile.ZipFile, excel_file_name: str) -> pd.DataFrame:
        """
        Função auxiliar para ler o arquivo Excel a partir de um objeto ZipFile.
        Calcula o GMIN para 2026 como o máximo entre 'Gtmin_Agente' e 'Gtmin_Eletrico'.
        """
        if excel_file_name not in zfile.namelist():
            print(f"⚠️ Arquivo Excel '{excel_file_name}' não encontrado no ZIP.")
            return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])
        
        try:
            with zfile.open(excel_file_name) as excel_file:
                df_excel = pd.read_excel(excel_file)
            print(f"✅ Arquivo Excel '{excel_file_name}' lido com sucesso.")
        except Exception as e:
            print(f"❌ Erro ao ler Excel: {e}")
            return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])
        
        df_excel.rename(columns={
            'código': 'ID',
            'nome': 'NOME_EXCEL',
            'mês': 'MES',
            'Gtmin_Agente': 'GTMIN_AGENTE',
            'Gtmin_Eletrico': 'GTMIN_ELETRICO'
        }, inplace=True)
        
        try:
            df_excel['MES_DT'] = pd.to_datetime(df_excel['MES'], format='%b/%y', errors='coerce')
            if df_excel['MES_DT'].isnull().any():
                df_excel['MES_DT'] = pd.to_datetime(df_excel['MES'], errors='coerce')
            
            df_excel.dropna(subset=['MES_DT'], inplace=True)
        except Exception as e:
            print(f"❌ Erro ao converter coluna 'mês': {e}")
            return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])
        
        df_2026 = df_excel[df_excel['MES_DT'].dt.year == 2026].copy()
        
        if df_2026.empty:
            print("⚠️ Nenhuma linha para 2026 no Excel.")
            return pd.DataFrame(columns=['ID', 'MES_ABBR', 'GMIN_NEW'])
        
        for col in ['GTMIN_AGENTE', 'GTMIN_ELETRICO']:
            if col in df_2026.columns:
                df_2026[col] = df_2026[col].astype(str).str.replace(',', '.', regex=False)
                df_2026[col] = pd.to_numeric(df_2026[col], errors='coerce').fillna(0)
        
        df_2026['GMIN_NEW'] = df_2026[['GTMIN_AGENTE', 'GTMIN_ELETRICO']].max(axis=1)
        
        # Mapeamento de meses
        month_abbr_map = {
            1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
            7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
        }
        df_2026['MES_ABBR'] = df_2026['MES_DT'].dt.month.map(month_abbr_map)
        
        return df_2026[['ID', 'MES_ABBR', 'GMIN_NEW']]
    
    def extrair_dados_historicos(self) -> pd.DataFrame:
        """
        Extrai dados de térmica do NEWAVE.
        Implementa a lógica de parse_termica.py
        """
        zip_path = self.base_dir / "termica" / "NW202511.zip"
        excel_file_name = "GTMIN_CCEE_112025.xlsx"
        
        if not zip_path.exists():
            aviso = f"Arquivo NEWAVE (Térmica) não encontrado: {zip_path}"
            self.metrics.avisos.append(aviso)
            return pd.DataFrame()
        
        print("🔥 Processando dados de Térmica do NEWAVE...")
        
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                # 1. Parsing TERM.DAT
                term_dat_files = [f for f in z.namelist() if os.path.basename(f).upper() == "TERM.DAT"]
                if not term_dat_files:
                    raise FileNotFoundError("TERM.DAT não encontrado no ZIP.")
                
                term_file_name = term_dat_files[0]
                term_lines = z.open(term_file_name).read().decode("latin1").splitlines()
                
                registros_term = []
                regex_term = re.compile(r"^\s*(\d+)\s+(.+?)\s{2,}(\d+\.?\d*)\s+(\d+\.?\d*)\s+([\d\.]+)\s+([\d\.]+)\s+(.*)$")
                meses = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"]
                
                print(f"  Lendo TERM.DAT...")
                for i, line in enumerate(term_lines):
                    if i < 2 or not line.strip():
                        continue
                    
                    match = regex_term.match(line)
                    if not match:
                        continue
                    
                    try:
                        usina_id = int(match.group(1))
                        nome = match.group(2).strip()
                        pot = float(match.group(3))
                        
                        valores_str = match.group(7).split()
                        gmin_term_values = []
                        for v_str in valores_str[:12]:
                            try:
                                gmin_term_values.append(float(v_str.replace(",", ".")))
                            except ValueError:
                                gmin_term_values.append(0.0)
                        
                        while len(gmin_term_values) < 12:
                            gmin_term_values.append(0.0)
                        
                        registro = {
                            "ID": usina_id,
                            "NOME": nome,
                            "GMAX": pot,
                        }
                        
                        for mes_abbr, valor in zip(meses, gmin_term_values):
                            registro[f"GMIN_{mes_abbr}"] = valor
                        
                        registros_term.append(registro)
                    except ValueError:
                        continue
                
                df_term = pd.DataFrame(registros_term)
                print(f"  ✓ TERM.DAT: {len(df_term)} usinas térmicas")
                
                # 2. Parsing CONFT.DAT
                df_conft = self._parse_conft_dat(z)
                
                # 3. Parsing CLAST.DAT
                df_clast = self._parse_clast_dat(z)
                
                # 4. Parsing Excel GTMIN
                df_gtmin_excel = self._parse_gtmin_excel(z, excel_file_name)
                
                # 5. Consolidar GMIN para 2026
                if not df_gtmin_excel.empty:
                    print("  Consolidando GMINs de 2026 do Excel...")
                    df_term_ids = set(df_term['ID'].unique())
                    
                    for _, row in df_gtmin_excel.iterrows():
                        usina_id = row['ID']
                        mes_abbr = row['MES_ABBR']
                        gmin_new_value = row['GMIN_NEW']
                        
                        if usina_id in df_term_ids:
                            col_name = f"GMIN_{mes_abbr}"
                            df_term.loc[df_term['ID'] == usina_id, col_name] = gmin_new_value
                    print("  ✓ GMINs atualizados para 2026")
                
                # 6. Merge dos DataFrames
                df_final = df_term.copy()
                
                if not df_conft.empty:
                    df_final = df_final.merge(df_conft, on='ID', how='left')
                
                if not df_clast.empty:
                    df_final = df_final.merge(df_clast, on='ID', how='left')
                
                self.metrics.data_metrics.dados_brutos = len(df_final)
                print(f"✅ Dados de Térmica extraídos: {len(df_final)} usinas")
                
                return df_final
        
        except Exception as e:
            aviso = f"Erro ao processar dados de Térmica: {e}"
            self.metrics.avisos.append(aviso)
            print(f"❌ {aviso}")
            return pd.DataFrame()
    
    def criar_curva_tipica(self, df_historico: pd.DataFrame) -> pd.DataFrame:
        """Térmica não usa curva típica, apenas passa os dados adiante."""
        return df_historico
    
    def gerar_projecao(self, curva_tipica: pd.DataFrame, mes: int) -> pd.DataFrame:
        """
        Gera projeção de térmica aplicando regra de despacho forçado.
        Implementa a lógica de agg_subsystem_termicas.py
        """
        if curva_tipica.empty:
            return pd.DataFrame()
        
        print(f"  Aplicando regra de despacho forçado (Mês {mes})...")
        
        df_mes = curva_tipica.copy()
        
        # 1. Adicionar CMO = 0.0
        df_mes['CMO'] = 0.0
        
        # 2. Renomear SSIS para Submercado_ID
        if 'SSIS' in df_mes.columns:
            df_mes = df_mes.rename(columns={'SSIS': 'Submercado_ID'})
        
        # 3. Identificar colunas GMIN
        gmin_cols = [col for col in df_mes.columns if col.startswith('GMIN_')]
        
        # 4. Aplicar regra: se CVU <= CMO => GMIN_MÊS = GMAX
        if 'CUSTO_CVU' in df_mes.columns:
            mask_despacho_forcado = df_mes['CUSTO_CVU'] <= df_mes['CMO']
            
            for col in gmin_cols:
                df_mes.loc[mask_despacho_forcado, col] = df_mes.loc[mask_despacho_forcado, 'GMAX']
            
            # 5. Garantir GMIN <= GMAX
            for col in gmin_cols:
                over = df_mes[col] > df_mes['GMAX']
                if over.any():
                    df_mes.loc[over, col] = df_mes.loc[over, 'GMAX']
        
        # Adicionar coluna de mês
        df_mes['Mes'] = mes
        
        return df_mes


class AggregatedProcessor:
    """Processador agregado que executa pipeline completo e agrega resultados."""
    
    def __init__(self, base_dir: Path, ano: int = 2026, config: Optional[ProcessingConfig] = None):
        """
        Inicializa o processador agregado.
        
        Args:
            base_dir: Diretório base onde estão os arquivos de entrada
            ano: Ano para processamento
            config: Configuração de processamento (se None, usa DEFAULT_CONFIG)
        """
        self.base_dir = Path(base_dir)
        self.ano = ano
        self.config = config or DEFAULT_CONFIG
        
        # Inicializar processadores com a mesma configuração
        self.processadores = {
            'carga': CargaProcessor(base_dir, ano, self.config),
            'eol_ufv_mmgd': EolUfvMmgdProcessor(base_dir, ano, self.config),
            'pch_pct': PchPctProcessor(base_dir, ano, self.config),
            'termica': TermicaProcessor(base_dir, ano, self.config),
        }
        
        print(f"\n🔧 Configuração de Processamento: {self.config.get_description()}")
        if REUSAR_CURVAS_TIPICAS:
            print(f"♻️  Modo: REUTILIZAR curvas típicas existentes (RÁPIDO)")
            print(f"   → Métricas de extração/limpeza NÃO serão coletadas")
            print(f"   → Apenas métricas de projeção e agregação")
        else:
            print(f"🔄 Modo: CRIAR novas curvas típicas (COMPLETO)")
            print(f"   → Todas as métricas serão coletadas")
            print(f"   → Processo completo de ponta a ponta")
        print()
    
    def processar_pipeline_completo(
        self,
        meses: List[int],
        output_dir: Optional[Path] = None
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Executa pipeline completo para todas as fontes e agrega resultados.
        
        Args:
            meses: Lista de meses a processar
            output_dir: Diretório para agregação final
            
        Returns:
            Dicionário com resultados por fonte
        """
        print("\n" + "="*80)
        print("PIPELINE COMPLETO DE PROCESSAMENTO")
        print("="*80)
        print(f"Período: {len(meses)} mês(es) | Ano: {self.ano}")
        print("="*80 + "\n")
        
        resultados = {}
        
        # Executar pipeline para cada fonte
        for nome_fonte, processador in self.processadores.items():
            print(f"\n{'*'*80}")
            print(f"FONTE: {nome_fonte.upper()}")
            print(f"{'*'*80}")
            
            try:
                resultado_fonte = processador.processar_pipeline_completo(meses)
                resultados[nome_fonte] = resultado_fonte
            except Exception as e:
                print(f"❌ Erro fatal ao processar {nome_fonte}: {str(e)}")
                resultados[nome_fonte] = {}
        
        # Etapa 4: Agregação Final
        periodo_info = None
        if output_dir:
            periodo_info = self._agregar_resultados_finais(meses, output_dir)
        
        print("\n" + "="*80)
        print("PIPELINE COMPLETO CONCLUÍDO")
        print("="*80 + "\n")
        
        return resultados, periodo_info
    
    def _agregar_resultados_finais(self, meses: List[int], output_dir: Path):
        """
        Agrega todos os resultados em arquivos finais (lógica adaptada do agg_input.py).
        - Gera formato melted para CARGA, PCH, PCT, EOL, UFV, MMGD
        - Salva TÉRMICA separadamente (não vai no melted)
        """
        print("\n" + "="*60)
        print("AGREGAÇÃO FINAL DE TODAS AS FONTES")
        print("="*60 + "\n")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Para armazenar as datas processadas
        todas_datas = []
        
        for month_num in meses:
            month_str = f"{month_num:02d}"
            year_str = str(self.ano)
            
            print(f"\n📦 Agregando mês {month_str}/{year_str}...")
            
            try:
                # 1. Carregar Carga (DataFrame Base)
                carga_file = self.base_dir / "carga" / f"resultados_{self.ano}" / f"forecast_carga_{month_str}-{year_str}.csv"
                
                if not carga_file.exists():
                    print(f"  ⚠️  Arquivo de Carga não encontrado: {carga_file.name}. Pulando este mês.")
                    continue
                
                carga_df = pd.read_csv(carga_file, sep=';', decimal=',')
                
                # Converter DataHora e armazenar datas
                if 'DataHora' in carga_df.columns:
                    carga_df['DataHora'] = pd.to_datetime(carga_df['DataHora'])
                    # Adicionar coluna DATA
                    carga_df['Data'] = carga_df['DataHora'].dt.date
                    # Coletar datas para estatísticas
                    todas_datas.extend(carga_df['Data'].unique())
                
                # Adicionar colunas auxiliares
                carga_df['Mes'] = month_num
                carga_df['Hora'] = carga_df['DataHora'].dt.hour if 'DataHora' in carga_df.columns else carga_df['Hora']
                
                # Mapear dia da semana
                if 'DataHora' in carga_df.columns:
                    carga_df['DiaDaSemana_PT'] = carga_df['DataHora'].dt.day_name().map(DAY_MAP_EN_TO_PT)
                
                # Garantir colunas necessárias
                if 'Flag_Feriado' not in carga_df.columns:
                    carga_df['Flag_Feriado'] = False
                if 'Patamar' not in carga_df.columns:
                    carga_df['Patamar'] = 'N/A'
                
                carga_df['Patamar'] = carga_df['Patamar'].str.upper() if 'Patamar' in carga_df.columns else 'N/A'
                
                # 2. Carregar PCH/PCT
                pch_file = self.base_dir / "pch_pct" / f"resultados_{self.ano}" / f"forecast_pch_pct_{month_str}-{self.ano}.csv"
                
                if pch_file.exists():
                    pch_pct_df = pd.read_csv(pch_file, sep=';', decimal=',')
                    
                    # Garantir que Carga tenha Tipo_Dia_Num (0-6 = seg-dom)
                    if 'DataHora' in carga_df.columns and 'Tipo_Dia_Num' not in carga_df.columns:
                        carga_df['Tipo_Dia_Num'] = carga_df['DataHora'].dt.dayofweek
                    
                    # Extrair colunas PCH
                    pch_cols = [col for col in pch_pct_df.columns if col.startswith('PCH -')]
                    if pch_cols:
                        pch_data = pch_pct_df[['Mes', 'Tipo_Dia_Num', 'Hora'] + pch_cols].copy()
                        pch_data.columns = ['Mes', 'Tipo_Dia_Num', 'Hora'] + [col.replace('PCH - ', 'PCH_') for col in pch_cols]
                        
                        # CORREÇÃO: Merge usando Mes + Tipo_Dia_Num + Hora
                        carga_df = pd.merge(
                            carga_df, pch_data,
                            on=['Mes', 'Tipo_Dia_Num', 'Hora'], how='left', suffixes=('', '_PCH')
                        )
                    
                    # Extrair colunas PCT
                    pct_cols = [col for col in pch_pct_df.columns if col.startswith('PCT -')]
                    if pct_cols:
                        pct_data = pch_pct_df[['Mes', 'Tipo_Dia_Num', 'Hora'] + pct_cols].copy()
                        pct_data.columns = ['Mes', 'Tipo_Dia_Num', 'Hora'] + [col.replace('PCT - ', 'PCT_') for col in pct_cols]
                        
                        # CORREÇÃO: Merge usando Mes + Tipo_Dia_Num + Hora
                        carga_df = pd.merge(
                            carga_df, pct_data,
                            on=['Mes', 'Tipo_Dia_Num', 'Hora'], how='left', suffixes=('', '_PCT')
                        )
                else:
                    print(f"  ⚠️  Arquivo PCH/PCT não encontrado: {pch_file.name}")
                    for col in ['PCH_SE', 'PCH_S', 'PCH_NE', 'PCH_N', 'PCT_SE', 'PCT_S', 'PCT_NE', 'PCT_N']:
                        carga_df[col] = np.nan
                
                # 3. Carregar EOL/UFV/MMGD (CSV já no formato melted!)
                eum_file = self.base_dir / "eol_uvf_mmgd" / "resultados_2026" / f"forecast_ufv_mmgd_eol_{month_str}_2026.csv"
                
                # EOL/UFV/MMGD já está no formato melted (MES, DIA_SEMANA, HORA, FONTE, SUBMERCADO, PERCENTIL, VALOR)
                # Não precisa pivotar, apenas adicionar ao resultado final
                
                # 4. Realizar MELT do DataFrame combinado (CARGA + PCH + PCT)
                df_melted_carga_pch_pct = self._melt_combined_dataframe(carga_df, month_num, year_str)
                
                # 5. Se existir EOL/UFV/MMGD, adicionar ao melted
                if eum_file.exists():
                    try:
                        eum_df = pd.read_csv(eum_file, sep=';')
                        
                        if not eum_df.empty and 'FONTE' in eum_df.columns:
                            # Renomear colunas para padrão do melted
                            eum_df_melted = eum_df.rename(columns={
                                'MES': 'Mes',
                                'DIA_SEMANA': 'DIA_SEMANA_NUM',
                                'HORA': 'HORA',
                                'FONTE': 'PARAMETRO',
                                'SUBMERCADO': 'SUBMERCADO',
                                'VALOR': 'VALOR'
                            })
                            
                            # Adicionar colunas faltantes para compatibilidade
                            eum_df_melted['ID_INPUT'] = df_melted_carga_pch_pct['ID_INPUT'].iloc[0] if not df_melted_carga_pch_pct.empty else f"PRED-{month_str}-{year_str}"
                            eum_df_melted['ID_SUBMERCADO'] = eum_df_melted['SUBMERCADO'].map({'SE': 1, 'S': 2, 'NE': 3, 'N': 4})
                            
                            # Selecionar apenas colunas necessárias que existem no melted padrão
                            colunas_finais = ['ID_INPUT', 'SUBMERCADO', 'ID_SUBMERCADO', 'HORA', 'DIA_SEMANA_NUM', 'PARAMETRO', 'VALOR']
                            eum_df_melted = eum_df_melted[[col for col in colunas_finais if col in eum_df_melted.columns]]
                            
                            # Combinar com melted de CARGA/PCH/PCT
                            df_melted = pd.concat([df_melted_carga_pch_pct, eum_df_melted], ignore_index=True)
                            print(f"  ✓ EOL/UFV/MMGD adicionado: {len(eum_df_melted):,} registros")
                        else:
                            df_melted = df_melted_carga_pch_pct
                    except Exception as e:
                        print(f"  ⚠️  Erro ao carregar EOL/UFV/MMGD: {e}")
                        df_melted = df_melted_carga_pch_pct
                else:
                    print(f"  ⚠️  Arquivo EOL/UFV/MMGD não encontrado: {eum_file.name}")
                    df_melted = df_melted_carga_pch_pct
                
                if not df_melted.empty:
                    # Salvar formato melted (CARGA + PCH + PCT + EOL + UFV + MMGD)
                    output_melted_file = output_dir / f"melted_input_{month_str}-{year_str}.csv"
                    df_melted.to_csv(output_melted_file, index=False, sep=';', decimal=',')
                    
                    # Contar registros por fonte no melted
                    if 'PARAMETRO' in df_melted.columns:
                        registros_por_fonte = df_melted['PARAMETRO'].value_counts().to_dict()
                        total_melted = len(df_melted)
                        print(f"  ✅ Arquivo melted salvo: {output_melted_file.name} ({total_melted:,} registros)")
                        print(f"     Detalhamento: {registros_por_fonte}")
                    else:
                        print(f"  ✅ Arquivo melted salvo: {output_melted_file.name} ({len(df_melted):,} registros)")
                else:
                    print(f"  ⚠️  Nenhum dado melted gerado para mês {month_str}")
                
                # 5. Salvar TÉRMICA separadamente
                self._salvar_termica_separadamente(month_num, year_str, output_dir)
            
            except Exception as e:
                print(f"  ❌ Erro ao agregar mês {month_str}: {e}")
        
        print("\n✅ Agregação final concluída!")
        
        # Retornar informações de período
        if todas_datas:
            todas_datas_sorted = sorted(set(todas_datas))
            return {
                'data_inicio': todas_datas_sorted[0],
                'data_fim': todas_datas_sorted[-1],
                'total_dias': len(todas_datas_sorted)
            }
        return None
    
    def _salvar_termica_separadamente(self, month_num: int, year_str: str, output_dir: Path):
        """
        Salva dados de Térmica em arquivo separado.
        Térmica NÃO entra no arquivo melted.
        """
        termica_file = self.base_dir / "termica" / f"resultados_{self.ano}" / f"forecast_termica_{month_num:02d}-{year_str}.csv"
        
        if not termica_file.exists():
            print(f"  ⚠️  Arquivo de Térmica não encontrado: {termica_file.name}")
            return
        
        try:
            termica_df = pd.read_csv(termica_file, sep=';', decimal=',')
            
            # Salvar cópia em outputs
            output_termica_file = output_dir / f"termica_{month_num:02d}-{year_str}.csv"
            termica_df.to_csv(output_termica_file, index=False, sep=';', decimal=',')
            
            print(f"  🔥 Térmica salva separadamente: {output_termica_file.name} ({len(termica_df):,} registros)")
        
        except Exception as e:
            print(f"  ❌ Erro ao salvar Térmica: {e}")
    
    def _melt_combined_dataframe(self, df_combined: pd.DataFrame, month_num: int, year_str: str) -> pd.DataFrame:
        """
        Realiza operação de melt no DataFrame combinado.
        Adaptado do agg_input.py
        """
        # Definir colunas identificadoras (incluindo Data)
        id_vars = ['Data', 'DataHora', 'Hora', 'DiaDaSemana_PT', 'Flag_Feriado', 'Patamar']
        
        # Definir colunas de valores
        # EOL/UFV/MMGD não são mais incluídos aqui, pois já vêm no formato melted
        carga_cols = ['SE', 'S', 'NE', 'N']
        param_prefixes = ['PCH', 'PCT']  # Removido MMGD, UFV, EOL
        submercados = ['SE', 'S', 'NE', 'N']
        
        value_vars = carga_cols[:]
        
        for prefix in param_prefixes:
            for sub in submercados:
                col_name = f"{prefix}_{sub}"
                if col_name in df_combined.columns:
                    value_vars.append(col_name)
        
        # Filtrar colunas existentes
        id_vars = [col for col in id_vars if col in df_combined.columns]
        value_vars = [col for col in value_vars if col in df_combined.columns]
        
        if not value_vars:
            print("  ⚠️  Nenhuma coluna de valor encontrada para melt")
            return pd.DataFrame()
        
        # Realizar melt
        df_melted = pd.melt(
            df_combined,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='original_param_submercado',
            value_name='VALOR'
        )
        
        # Criar colunas PARAMETRO e SUBMERCADO
        def parse_param_submercado(col_val):
            parts = col_val.split('_')
            if len(parts) == 1:  # Coluna de carga
                param = 'CARGA'
                submercado = parts[0]
            elif len(parts) == 2:
                param = parts[0]
                submercado = parts[1]
            else:
                param = 'UNKNOWN'
                submercado = col_val
            return param.upper(), submercado.upper()
        
        df_melted[['PARAMETRO', 'SUBMERCADO']] = df_melted['original_param_submercado'].apply(
            lambda x: pd.Series(parse_param_submercado(x))
        )
        
        # Mapear SUBMERCADO para ID_SUBMERCADO
        submercado_id_map = {'SE': 1, 'S': 2, 'NE': 3, 'N': 4}
        df_melted['ID_SUBMERCADO'] = df_melted['SUBMERCADO'].map(submercado_id_map)
        
        # Criar ID_INPUT
        month_abbr_map = {
            1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
            7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
        }
        month_abbr = month_abbr_map.get(month_num, '')
        df_melted['ID_INPUT'] = f"PRED-{month_abbr}-{year_str}"
        
        # Renomear colunas
        df_melted = df_melted.rename(columns={
            'Data': 'DATA',
            'DataHora': 'TIMESTAMP',
            'Hora': 'HORA',
            'DiaDaSemana_PT': 'DIA_SEMANA'
        })
        
        # Remover acentos de TERÇA e SÁBADO
        if 'DIA_SEMANA' in df_melted.columns:
            df_melted['DIA_SEMANA'] = df_melted['DIA_SEMANA'].apply(
                lambda texto: ''.join(
                    char for char in unicodedata.normalize('NFD', str(texto))
                    if unicodedata.category(char) != 'Mn'
                ) if pd.notna(texto) else texto
            )
        
        # Adicionar DIA_SEMANA_NUM
        day_name_to_num = {
            'SEGUNDA': 0, 'TERCA': 1, 'QUARTA': 2, 'QUINTA': 3,
            'SEXTA': 4, 'SABADO': 5, 'DOMINGO': 6
        }
        if 'DIA_SEMANA' in df_melted.columns:
            df_melted['DIA_SEMANA_NUM'] = df_melted['DIA_SEMANA'].map(day_name_to_num)
        
        # Reordenar colunas (incluindo DATA)
        final_cols = [
            'ID_INPUT', 'DATA', 'SUBMERCADO', 'ID_SUBMERCADO', 'TIMESTAMP', 'HORA',
            'DIA_SEMANA_NUM', 'DIA_SEMANA', 'Flag_Feriado', 'Patamar', 'PARAMETRO', 'VALOR'
        ]
        
        df_melted = df_melted[[col for col in final_cols if col in df_melted.columns]]
        df_melted.drop(columns=['original_param_submercado'], inplace=True, errors='ignore')
        
        return df_melted
    
    def obter_metricas(self) -> Dict[str, SourceMetrics]:
        """Retorna as métricas de todas as fontes."""
        return {
            nome: proc.metrics
            for nome, proc in self.processadores.items()
        }
