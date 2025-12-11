# Especificação Técnica - Sistema de Pré-processamento de Dados

## Trabalho de Conclusão de Curso

---

## 1. Introdução

### 1.1 Objetivo do Sistema

Este documento descreve a especificação técnica de um sistema de pré-processamento de dados desenvolvido para otimização de curtailment no Sistema Interligado Nacional (SIN). O sistema foi projetado para:

1. Processar dados de múltiplas fontes de forma eficiente
2. Coletar métricas detalhadas para análise de desempenho
3. Fornecer uma base quantitativa para pesquisa acadêmica
4. Permitir reprodutibilidade de experimentos

### 1.2 Escopo

O sistema processa dados de quatro fontes principais:
- **Carga**: Previsão de demanda de energia elétrica
- **EOL/UFV/MMGD**: Geração renovável (eólica, solar, micro/mini geração)
- **PCH/PCT**: Pequenas centrais (hidrelétricas e termelétricas)
- **Térmica**: Usinas termelétricas convencionais

---

## 2. Arquitetura do Sistema

### 2.1 Visão Geral

```
┌─────────────────────────────────────────────────────────────┐
│                    main.py (Orquestrador)                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              AggregatedProcessor (Agregador)                │
└───┬──────────┬──────────┬──────────┬──────────────────────┘
    │          │          │          │
    ▼          ▼          ▼          ▼
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│ Carga  │ │  EOL/  │ │ PCH/   │ │Térmica │
│Processor│ │UFV/MMGD│ │  PCT   │ │Processor│
└────────┘ └────────┘ └────────┘ └────────┘
    │          │          │          │
    └──────────┴──────────┴──────────┘
                  │
                  ▼
         ┌────────────────┐
         │SourceMetrics   │
         └────────────────┘
                  │
                  ▼
         ┌────────────────┐
         │ProcessingMetrics│
         └────────────────┘
```

### 2.2 Componentes Principais

#### 2.2.1 Módulo de Métricas (`src/metrics.py`)

**Classes:**

1. **DataMetrics**: Rastreia quantidade de dados
   - `dados_extraidos`: Registros lidos da fonte
   - `dados_limpos`: Registros após limpeza
   - `dados_curva_tipica`: Registros para curva típica
   - `dados_projecao`: Registros para projeção
   - `dados_finais`: Registros no output final

2. **PerformanceMetrics**: Rastreia desempenho
   - `tempo_total_segundos`: Duração do processamento
   - `memoria_inicial_mb`: Memória no início
   - `memoria_final_mb`: Memória no final
   - `memoria_pico_mb`: Pico de memória
   - `memoria_media_mb`: Média de uso

3. **SourceMetrics**: Métricas por fonte
   - Combina DataMetrics e PerformanceMetrics
   - Registra avisos e erros
   - Lista arquivos processados

4. **ProcessingMetrics**: Métricas agregadas
   - Consolida todas as fontes
   - Calcula totais e médias
   - Gera relatórios formatados

#### 2.2.2 Módulo de Processadores (`src/data_processor.py`)

**Classes Base:**

1. **BaseDataProcessor**: Classe abstrata
   ```python
   - get_source_name() -> str
   - processar_mes(mes: int) -> pd.DataFrame
   - processar_periodo(meses: List[int]) -> pd.DataFrame
   ```

**Processadores Específicos:**

1. **CargaProcessor**
   - Fonte: `carga/resultados_YYYY/forecast_carga_MM-YYYY.csv`
   - Formato: CSV com separador `;`, decimal `,`
   - Colunas: DataHora, SE, S, NE, N, Flag_Feriado, Patamar

2. **EolUfvMmgdProcessor**
   - Fonte: `eol_uvf_mmgd/resultados_2026/forecast_ufv_mmgd_eol_MM_2026.csv`
   - Formato: CSV com separador `;`
   - Colunas: MES, DIA_SEMANA, HORA, FONTE, SUBMERCADO, PERCENTIL, VALOR

3. **PchPctProcessor**
   - Fonte: `pch-pct/ResultadosPCH-PCT_YYYY/forecast_{PCH|PCT}_MM-YYYY.csv`
   - Formato: CSV com separador `;`, decimal `,`
   - Processa PCH e PCT separadamente e faz merge

4. **TermicaProcessor**
   - Fonte: `termica/termica.csv`
   - Formato: CSV com separador `;`
   - Cache interno (dados não variam por mês)

**Agregador:**

5. **AggregatedProcessor**
   - Coordena todos os processadores
   - Executa processamento em sequência
   - Coleta métricas de todas as fontes
   - Salva outputs em diretório especificado

---

## 3. Pipeline de Processamento

### 3.1 Fluxo de Dados

```
1. Leitura      → 2. Validação → 3. Limpeza → 4. Transformação → 5. Output
   (arquivo)       (tipos)         (outliers)    (formato)         (CSV/JSON)
```

### 3.2 Etapas Detalhadas

#### Etapa 1: Leitura
- Localiza arquivo de entrada
- Detecta formato (CSV/Excel)
- Aplica encoding correto (UTF-8, Latin-1)
- Atualiza `dados_extraidos`

#### Etapa 2: Validação
- Verifica colunas obrigatórias
- Converte tipos de dados
- Trata valores com vírgula decimal
- Registra avisos se dados ausentes

#### Etapa 3: Limpeza
- Remove registros com NaN em colunas críticas
- Filtra outliers (valores não positivos)
- Remove duplicatas se existirem
- Atualiza `dados_limpos`

#### Etapa 4: Transformação
- Aplica transformações específicas da fonte
- Normaliza formatos de data/hora
- Padroniza nomes de colunas
- Atualiza `dados_projecao`

#### Etapa 5: Output
- Salva DataFrame em CSV
- Gera arquivo de métricas JSON
- Atualiza `dados_finais`

### 3.3 Coleta de Métricas

**Pontos de Amostragem:**

1. **Início do processamento**:
   - `performance_metrics.iniciar()`
   - Captura tempo e memória inicial

2. **Durante processamento**:
   - `performance_metrics.amostrar_memoria()` após cada mês
   - Atualiza memória pico

3. **Fim do processamento**:
   - `performance_metrics.finalizar()`
   - Calcula tempo total, média de memória

---

## 4. Cenários de Teste

### 4.1 Especificação dos Cenários

| Cenário | Meses | Registros Esperados* | Tempo Estimado** | Uso de Memória*** |
|---------|-------|----------------------|------------------|-------------------|
| 1 mês | Jan | ~500 por fonte | < 1 min | ~200 MB |
| 3 meses | Jan-Mar | ~1,500 por fonte | 1-3 min | ~300 MB |
| 6 meses | Jan-Jun | ~3,000 por fonte | 3-6 min | ~500 MB |
| 12 semanas | Jan-Dez | ~2,000 por fonte | 5-10 min | ~800 MB |

\* Valores aproximados dependem dos dados disponíveis
\** Baseado em processador Intel i5, 8GB RAM, SSD
\*** Pico de memória durante processamento

### 4.2 Métricas Coletadas por Cenário

Para cada cenário, o sistema coleta:

**Métricas de Volume:**
- Total de registros extraídos
- Total de registros após limpeza
- Percentual de perda de dados
- Distribuição de dados por fonte

**Métricas de Desempenho:**
- Tempo total de execução
- Tempo por fonte de dados
- Throughput (registros/segundo)
- Eficiência de memória (registros/MB)

**Métricas de Qualidade:**
- Número de avisos gerados
- Número de erros encontrados
- Taxa de sucesso por fonte
- Completude dos dados

---

## 5. Formato de Dados

### 5.1 Formato de Entrada

#### Carga
```csv
DataHora;SE;S;NE;N;Flag_Feriado;Patamar
2026-01-01 00:00:00;46187,5;14210,2;12913,4;7479,1;False;LEVE
```

#### EOL/UFV/MMGD
```csv
MES;DIA_SEMANA;HORA;FONTE;SUBMERCADO;PERCENTIL;VALOR
1;0;0;EOL;SE;50;1234.56
```

#### PCH/PCT
```csv
Mes;Tipo_Dia_Num;Hora;PCH - SE;PCH - S;PCH - NE;PCH - N
1;0;0;2735,1;948,2;88,3;162,4
```

#### Térmica
```csv
ID;NOME;Submercado_ID;GMAX;GMIN_JAN;GMIN_FEV;...
1;ANGRA 1;1;640;320;320;...
```

### 5.2 Formato de Saída

#### Dados Processados (CSV)
- Separador: `;`
- Decimal: `,`
- Encoding: UTF-8
- Formato padronizado por fonte

#### Métricas (JSON)
```json
{
  "timestamp_inicio": "2025-11-15T10:30:00",
  "timestamp_fim": "2025-11-15T10:35:23",
  "cenario": "3_meses",
  "periodo_inicio": "01/2026",
  "periodo_fim": "03/2026",
  "tempo_total_segundos": 323.45,
  "dados_totais_finais": 15120,
  "metricas_por_fonte": {
    "carga": {
      "data_metrics": {...},
      "performance_metrics": {...}
    },
    ...
  }
}
```

---

## 6. Requisitos Técnicos

### 6.1 Hardware

**Mínimo:**
- Processador: Intel i3 ou equivalente
- RAM: 4 GB
- Disco: 1 GB livre (SSD recomendado)

**Recomendado:**
- Processador: Intel i5 ou superior
- RAM: 8 GB ou mais
- Disco: 5 GB livre (SSD)

### 6.2 Software

**Obrigatório:**
- Python 3.8 ou superior
- pandas >= 2.0.0
- numpy >= 1.24.0
- psutil >= 5.9.0

**Opcional:**
- matplotlib >= 3.7.0 (para visualizações)
- jupyter (para análise interativa)

### 6.3 Sistema Operacional

- Linux (testado em Ubuntu 20.04+)
- Windows 10/11
- macOS 10.15+

---

## 7. Métricas de Avaliação

### 7.1 Métricas Primárias

1. **Tempo de Processamento**: T_total (segundos)
2. **Throughput**: R = N_registros / T_total (registros/s)
3. **Eficiência de Memória**: E = N_registros / M_pico (registros/MB)
4. **Taxa de Sucesso**: S = N_processados / N_total (%)

### 7.2 Métricas Secundárias

1. **Escalabilidade**: ΔT / ΔN (variação tempo por registro adicional)
2. **Overhead de Memória**: M_delta = M_final - M_inicial (MB)
3. **Taxa de Perda**: L = (N_extraidos - N_finais) / N_extraidos (%)
4. **Confiabilidade**: C = 1 - (N_erros / N_operacoes)

### 7.3 Análise Comparativa

Para comparar cenários, utiliza-se:

```
Score = (w1 * R_normalizado) + 
        (w2 * E_normalizado) + 
        (w3 * S) + 
        (w4 * C)

onde: w1 + w2 + w3 + w4 = 1
```

Pesos sugeridos:
- w1 (throughput) = 0.3
- w2 (eficiência) = 0.2
- w3 (sucesso) = 0.3
- w4 (confiabilidade) = 0.2

---

## 8. Testes e Validação

### 8.1 Testes Unitários

Implementados em `tests/test_scenarios.py`:

1. `test_1_cenario_1_mes`: Valida processamento mínimo
2. `test_2_cenario_3_meses`: Valida período curto
3. `test_3_cenario_6_meses`: Valida período médio
4. `test_4_cenario_12_semanas_tipicas`: Valida ano completo

### 8.2 Critérios de Validação

**Para cada teste:**
- [ ] Métricas não são nulas
- [ ] Tempo de processamento > 0
- [ ] Dados finais > 0
- [ ] Sem erros críticos
- [ ] Arquivos de saída existem

### 8.3 Execução dos Testes

```bash
# Executar todos os testes
python tests/test_scenarios.py

# Gerar relatório comparativo
ls outputs/testes/comparacao_cenarios.csv
```

---

## 9. Análise de Resultados

### 9.1 Relatórios Gerados

1. **Por Execução:**
   - `metricas.json`: Métricas detalhadas
   - `*_processado.csv`: Dados processados por fonte

2. **Após Testes:**
   - `comparacao_cenarios.csv`: Comparativo quantitativo
   - `resultados_completos.json`: Todos os cenários em JSON

### 9.2 Visualizações

Geradas por `visualizar_metricas.py`:

1. `tempo_por_fonte.png`: Tempo de processamento
2. `memoria_por_fonte.png`: Uso de memória
3. `dados_por_fonte.png`: Volume de dados
4. `pipeline_dados.png`: Funil de processamento
5. `comparacao_tempo.png`: Comparação entre cenários
6. `comparacao_dados.png`: Volume entre cenários

### 9.3 Interpretação

**Tempo de Processamento:**
- Linear: O(n) - Bom
- Quadrático: O(n²) - Requer otimização
- Constante: O(1) - Ideal (pouco provável)

**Memória:**
- Crescimento linear: Normal
- Crescimento exponencial: Problema de memory leak
- Constante: Cache eficiente

**Perda de Dados:**
- < 5%: Aceitável
- 5-10%: Revisar limpeza
- > 10%: Problema crítico

---