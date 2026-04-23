# 🚖 NYC Taxi Data Pipeline & Dashboard

Este projeto é focado na construção de um pipeline ETL completo, desde a extração de dados brutos até a visualização em um dashboard interativo. Desenvolvido como parte do **Desafio Técnico 02** da comunidade **Dados Por Todos**.

O objetivo principal foi entender na prática o fluxo de dados: extrair, validar, transformar e carregar (ETL) para análise de larga escala utilizando o ecossistema Google Cloud.

---

## 🚀 Tecnologias Utilizadas

O projeto foi construído utilizando uma stack moderna de Engenharia de Dados:

* **Linguagem Principal:** Python 3.12+
* **Processamento de Dados:** Pandas & Pyarrow (para manipulação eficiente de arquivos Parquet)
* **Data Warehouse:** Google BigQuery (armazenamento de ~15 milhões de linhas)
* **Visualização:** Streamlit (Criação de dashboard interativo)
* **Infraestrutura:** Docker (Containerização do App) e WSL2 (Ambiente Linux no Windows)
* **Orquestração/Ambiente:** UV (Gerenciamento de dependências ultra-rápido)

---

## 📊 O Dashboard

O dashboard desenvolvido em Streamlit permite analisar:
- **Picos de Demanda:** Identificação de horários de maior fluxo em NYC.
- **Análise Financeira:** Ticket médio, faturamento por empresa de tecnologia (Creative Mobile vs Verifone) e gorjetas.
- **Detecção de Anomalias:** Filtro para viagens com padrões suspeitos (distâncias ou valores fora da curva).
- **Geolocalização:** Mapa de calor de pickups pela cidade.

---

## 🏗️ Arquitetura e Desafios Técnicos

### Lidando com Big Data (~15M de linhas)
Um dos maiores desafios foi processar e visualizar um volume de **2.93 GB** de dados brutos no BigQuery sem estourar o limite de memória (RAM) local. 

**Soluções implementadas:**
1.  **Otimização de Query:** Uso de `LIMIT` e filtros temporais diretamente no SQL para reduzir o tráfego de rede.
2.  **Caching:** Utilização do `@st.cache_data` do Streamlit para evitar chamadas repetitivas ao BigQuery, acelerando a navegação do usuário.
3.  **Dockerização:** O dashboard roda isolado em um container, facilitando o deploy e garantindo que as dependências sejam as mesmas em qualquer máquina.

---

## 🛠️ Como Executar o Projeto

1. **Clone o repositório:**
   ```bash
   git clone [https://github.com/felipeziviani/nyc-taxi-pipeline.git](https://github.com/felipeziviani/nyc-taxi-pipeline.git)

2. **Configure suas credenciais**
Crie um arquivo .env na raiz do projeto e adicione suas chaves do Google Cloud (certifique-se de colocar sua chave JSON na pasta /keys):
    ```env
    GOOGLE_APPLICATION_CREDENTIALS=./keys/sua_chave.json
    GOOGLE_CLOUD_PROJECT=seu-projeto-id
3. **Suba o ambiente com Docker**
    ```bash
    # Build da imagem
    docker build -t nyc-taxi-app .

    # Execução do container
    docker run -p 8501:8501 --env-file .env nyc-taxi-app

## 🤝 Créditos e Comunidade

Este projeto foi desenvolvido com o apoio e materiais da comunidade **Dados Por Todos (DPT)**. Agradecimentos especiais a **Laura** e **Luiza** (Co-founder e CEO da DPT & DPE) pela curadoria do conteúdo, auxiliando na produtividade e no desenvolvimento de portfólios de qualidade para a área de dados.

* **LinkedIn da comunidade:** [Dados por Todos](linkedin.com/company/dadosportodos/)
* **Documentação no Notion:** [Desafio Técnico 02 - Dados Por Todos](https://dadosportodos.notion.site/02-desafio-tecnico?p=331d9128b44981e19aaedb4537b90fc8&pm=s)

## 🚀 Novidades desta Versão
- **Modelagem Preditiva:** Adicionada aba de IA utilizando **XGBoost** para previsão de demanda.
- **Dockerização:** Agora o projeto é 100% containerizado para facilitar o deploy.
- **BigQuery Integration:** Otimização das queries para lidar com volumes de dados históricos.

## 🐳 Como rodar com Docker
1. Certifique-se de ter o arquivo `google_credentials.json` na raiz.
2. Construa a imagem:
   `docker build -t taxi-dashboard .`
3. Rode o container:
   `docker run -p 8501:8501 --env-file .env -v $(pwd)/google_credentials.json:/app/google_credentials.json taxi-dashboard`