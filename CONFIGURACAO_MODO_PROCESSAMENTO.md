# Configuração do Modo de Processamento

Este documento explica como configurar o modo de processamento de dados: **Semanas Típicas** vs **Período Completo** e **Reutilização de Curvas Típicas**.

---

## 📋 Modos Disponíveis

### 1. **Semana Típica** (Padrão)
- Processa **apenas 7 dias** (uma semana típica) por mês
- Busca uma semana completa de segunda a domingo
- **Uso ideal**: Cenários de análise rápida, testes, validações
- **Volume de dados**: ~168 horas por mês (7 dias × 24 horas)

### 2. **Período Completo**
- Processa **todos os dias** do período especificado
- Pode processar:
  - Mês completo (28-31 dias)
  - Número fixo de dias (ex: 30 dias, 90 dias)
- **Uso ideal**: Análises detalhadas, simulações completas, dados para modelo de otimização
- **Volume de dados**: Variável conforme configuração

### 3. **Reutilizar Curvas Típicas** 
- **PULA** a etapa de criação de curvas típicas
- Carrega curvas já existentes e vai direto para projeção
- **Uso ideal**: Testes rápidos, iterações de desenvolvimento, quando os dados históricos não mudaram

---

## 🛠️ Como Configurar

### Localização das Configurações

Abra o arquivo: `src/data_processor.py`

Localize as seções de configuração:

#### **1. Modo de Período** (linhas 101-104)
```python
DEFAULT_CONFIG = ProcessingConfig(
    mode=PeriodMode.SEMANA_TIPICA,  # Altere aqui
    dias_por_mes=None                # Altere aqui
)
```

#### **2. Reutilização de Curvas** (linha 126)
```python
REUSAR_CURVAS_TIPICAS = False  # Altere para True para reutilizar curvas existentes
```

---

## 📝 Exemplos de Configuração

### Cenário 1: Primeira Execução - Criando Tudo do Zero
```python
# Modo: Semana Típica
DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.SEMANA_TIPICA)
REUSAR_CURVAS_TIPICAS = False  # Criar curvas
```
**Resultado**: 
- ✅ Baixa dados do ONS
- ✅ Cria curvas típicas
- ✅ Gera projeções
- ⏱️ Tempo: ~10-15 minutos (depende da internet)
- 📊 Métricas: **COMPLETAS**

---

### Cenário 2: Testes Rápidos - Reutilizando Curvas
```python
# Modo: Semana Típica
DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.SEMANA_TIPICA)
REUSAR_CURVAS_TIPICAS = True  # Reutilizar curvas existentes
```
**Resultado**: 
- ❌ NÃO baixa dados do ONS
- ♻️ Reutiliza curvas típicas existentes
- ✅ Gera projeções
- ⏱️ Tempo: ~1-2 minutos (80-90% mais rápido!)
- 📊 Métricas: Apenas **projeção e agregação**

---

### Cenário 3: Análise Completa para TCC - Mês Inteiro
```python
# Modo: Período Completo (mês inteiro)
DEFAULT_CONFIG = ProcessingConfig(
    mode=PeriodMode.PERIODO_COMPLETO,
    dias_por_mes=None  # Mês completo
)
REUSAR_CURVAS_TIPICAS = False  # Criar curvas (primeira vez)
```
**Resultado**: 
- ✅ Baixa dados do ONS
- ✅ Cria curvas típicas
- ✅ Gera projeções para ~30 dias
- ⏱️ Tempo: ~10-15 minutos
- 📊 Métricas: **COMPLETAS**
- 💾 Dados: ~720 horas por mês

---

### Cenário 4: Iterações Rápidas - Mês Completo com Curvas Reutilizadas
```python
# Modo: Período Completo (mês inteiro)
DEFAULT_CONFIG = ProcessingConfig(
    mode=PeriodMode.PERIODO_COMPLETO,
    dias_por_mes=None
)
REUSAR_CURVAS_TIPICAS = True  # Reutilizar curvas
```
**Resultado**: 
- ♻️ Reutiliza curvas típicas
- ✅ Gera projeções para ~30 dias
- ⏱️ Tempo: ~2-3 minutos
- 📊 Métricas: Apenas **projeção e agregação**
- 💾 Dados: ~720 horas por mês

---

### Cenário 5: 90 Dias Corridos (3 meses)
```python
DEFAULT_CONFIG = ProcessingConfig(
    mode=PeriodMode.PERIODO_COMPLETO,
    dias_por_mes=90
)
REUSAR_CURVAS_TIPICAS = True  # Reutilizar curvas (mais rápido)
```
**Resultado**: 
- ♻️ Reutiliza curvas típicas
- ✅ Gera 90 dias seguidos
- ⏱️ Tempo: ~3-4 minutos
- 💾 Dados: 2.160 horas

⚠️ Executar com `meses=[1]` (processa 90 dias a partir de janeiro)

---

## 🚀 Workflow Recomendado para TCC

### Passo 1: Primeira Execução (Criar Curvas)
```python
DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.SEMANA_TIPICA)
REUSAR_CURVAS_TIPICAS = False
```
Execute: `python main.py`

**Resultado**: Curvas típicas criadas e salvas em `*/resultados_2026/curva_tipica_*.csv`

---

### Passo 2: Teste Semana Típica
```python
DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.SEMANA_TIPICA)
REUSAR_CURVAS_TIPICAS = True  # RÁPIDO!
```
Execute: `python main.py`

**Resultado**: Dados de 1 semana por mês em ~2 min

---

### Passo 3: Teste Mês Completo
```python
DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.PERIODO_COMPLETO, dias_por_mes=None)
REUSAR_CURVAS_TIPICAS = True  # RÁPIDO!
```
Execute: `python main.py`

**Resultado**: Dados completos do mês em ~3 min

---

### Passo 4: Análise Final para TCC
```python
DEFAULT_CONFIG = ProcessingConfig(mode=PeriodMode.PERIODO_COMPLETO, dias_por_mes=None)
REUSAR_CURVAS_TIPICAS = False  # Métricas COMPLETAS
```
Execute: `python main.py`

**Resultado**: Métricas completas para o TCC em ~15 min

---

## 🔍 Como Saber Qual Modo Está Ativo?

Ao executar, você verá mensagens como:

### Modo Completo (Criando Curvas):
```
🔧 Configuração de Processamento: Semana Típica (7 dias por mês)
🔄 Modo: CRIAR novas curvas típicas (COMPLETO)
   → Todas as métricas serão coletadas
   → Processo completo de ponta a ponta
```

### Modo Rápido (Reutilizando Curvas):
```
🔧 Configuração de Processamento: Período Completo (mês inteiro)
♻️  Modo: REUTILIZAR curvas típicas existentes (RÁPIDO)
   → Métricas de extração/limpeza NÃO serão coletadas
   → Apenas métricas de projeção e agregação
```

E durante o processamento de cada fonte:
```
♻️  REUTILIZANDO curva típica existente: curva_tipica_carga.csv
   ✅ 2016 registros carregados da curva típica
   ♻️  Curva típica reutilizada (economia de tempo!)
```

---

## ⚙️ Localização dos Arquivos de Curva Típica

As curvas típicas são salvas em:
- `carga/resultados_2026/curva_tipica_carga.csv`
- `pch_pct/resultados_2026/curva_tipica_pch_pct.csv`
- `eol_ufv_mmgd/resultados_2026/curva_tipica_eol_ufv_mmgd.csv` (cenários)
- `termica/resultados_2026/curva_tipica_termica.csv`

Você pode **inspecionar, editar ou deletar** esses arquivos conforme necessário.

---

## 💡 Dicas Importantes

### ✅ **Use REUSAR_CURVAS_TIPICAS = True quando:**
- Estiver testando diferentes períodos (semana vs mês)
- Estiver iterando no desenvolvimento
- Os dados históricos não mudaram
- Quiser focar em métricas de projeção/agregação
- Precisar de resultados rápidos

### ❌ **Use REUSAR_CURVAS_TIPICAS = False quando:**
- For a primeira execução
- Dados históricos do ONS foram atualizados
- Precisar de métricas COMPLETAS para o TCC
- Quiser validar todo o pipeline
- Houver mudanças nos dados do NEWAVE

### 🔄 **Forçar Recriação de Curvas:**
Delete os arquivos `curva_tipica_*.csv` e execute com `REUSAR_CURVAS_TIPICAS = False`
