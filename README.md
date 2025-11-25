# Sistema de Pré-processamento de Dados para Otimização de Curtailment

## 📋 Descrição

Este sistema foi desenvolvido como parte de um Trabalho de Conclusão de Curso (TCC) e implementa um pipeline completo de pré-processamento de dados para otimização de curtailment no Sistema Interligado Nacional (SIN).

O sistema processa dados de múltiplas fontes:
- **Carga**: Previsão de demanda de energia por submercado
- **EOL/UFV/MMGD**: Geração eólica, solar fotovoltaica e mini/micro geração distribuída
- **PCH/PCT**: Pequenas Centrais Hidrelétricas e Pequenas Centrais Termelétricas
- **Térmica**: Usinas termelétricas e seus parâmetros operacionais

## 🎯 Objetivos

- Agregar dados de diferentes fontes em um formato unificado
- Coletar métricas detalhadas de processamento para análise de desempenho
- Fornecer testes para diferentes cenários de uso
- Documentar quantitativamente o processo de transformação de dados

## 🏗️ Estrutura do Projeto

```
input_processor/
├── main.py                      # Programa principal
├── requirements.txt             # Dependências do projeto
├── README.md                    # Esta documentação
├── src/
│   ├── __init__.py
│   ├── metrics.py              # Sistema de métricas
│   └── data_processor.py       # Processadores de dados
├── tests/
│   └── test_scenarios.py       # Testes de cenários
├── carga/                       # Dados de carga
├── eol_uvf_mmgd/               # Dados de EOL/UFV/MMGD
├── pch-pct/                    # Dados de PCH/PCT
├── termica/                    # Dados de térmica
└── outputs/                    # Arquivos de saída (gerados)
```

## 🚀 Instalação

### Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)

### Passos

1. Clone o repositório ou navegue até o diretório do projeto:
```bash
cd /home/laral/repos/curtailment/regulatory-curtailment/input_processor
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

## 📊 Uso

### Execução do Programa Principal

Para processar dados com configuração padrão (1 mês):

```bash
python main.py
```

### Execução dos Testes de Cenário

Para executar todos os testes de cenário e gerar relatórios comparativos:

```bash
python tests/test_scenarios.py
```

Ou usando unittest:

```bash
python -m unittest tests.test_scenarios
```

### Cenários de Teste

O sistema implementa 4 cenários de teste:

1. **1 Mês**: Processa dados de janeiro/2026
2. **3 Meses**: Processa dados de janeiro a março/2026
3. **6 Meses**: Processa dados de janeiro a junho/2026
4. **12 Semanas Típicas**: Processa semanas típicas de todos os 12 meses do ano

## 📈 Métricas Coletadas

Para cada cenário, o sistema coleta e reporta:

### Métricas de Dados
- **Dados Extraídos**: Quantidade total de registros lidos da fonte
- **Dados Limpos**: Registros após remoção de outliers e valores inválidos
- **Dados Curva Típica**: Registros usados para criar curvas típicas
- **Dados Projeção**: Registros usados na projeção de geração
- **Dados Finais**: Registros no arquivo final processado

### Métricas de Desempenho
- **Tempo Total**: Tempo total de processamento (segundos e minutos)
- **Tempo por Fonte**: Tempo de processamento de cada fonte individual
- **Memória Inicial**: Uso de memória no início do processamento
- **Memória Final**: Uso de memória ao final do processamento
- **Memória Pico**: Pico de uso de memória durante o processamento
- **Memória Média**: Uso médio de memória durante o processamento

### Outras Métricas
- **Arquivos Processados**: Lista de arquivos lidos por fonte
- **Avisos**: Contagem de avisos gerados durante o processamento
- **Erros**: Contagem de erros encontrados

## 📂 Saídas Geradas

### Por Execução

Cada execução gera:
- Arquivos CSV processados por fonte (`outputs/processamento_YYYYMMDD_HHMMSS/`)
- Arquivo JSON com métricas detalhadas (`outputs/processamento_YYYYMMDD_HHMMSS/metricas.json`)

### Após Testes

Os testes geram:
- Arquivos processados para cada cenário (`outputs/testes/[cenario]/`)
- Relatório comparativo (`outputs/testes/comparacao_cenarios.csv`)
- Resultados completos em JSON (`outputs/testes/resultados_completos.json`)

## 🔬 Aplicação em Pesquisa

Este sistema foi desenvolvido especificamente para:

1. **Análise de Desempenho**: Comparar diferentes configurações de processamento
2. **Otimização de Pipeline**: Identificar gargalos no processamento
3. **Documentação Quantitativa**: Fornecer dados precisos sobre transformação de dados
4. **Reprodutibilidade**: Garantir que os resultados possam ser reproduzidos

### Métricas

As métricas coletadas podem ser usadas para:
- Análise de escalabilidade (1 mês vs 12 meses)
- Identificação de fontes mais custosas computacionalmente
- Projeção de requisitos para processamento em produção
- Comparação de diferentes abordagens de processamento

## 📝 Licença

Este projeto foi desenvolvido como parte de um Trabalho de Conclusão de Curso.

## 👨‍💻 Autor

Lara Ramos Linhares
Ano: 2025

---

**Nota**: Os dados processados por este sistema são utilizados em modelos de otimização de curtailment para o Sistema Interligado Nacional (SIN) brasileiro.

