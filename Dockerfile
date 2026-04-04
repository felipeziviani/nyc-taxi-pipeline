# Usa uma imagem oficial do Python (versão slim para ser mais leve)
FROM python:3.12-slim

# Define a pasta de trabalho dentro do Docker
WORKDIR /app

# Instala ferramentas básicas do sistema (necessário para compilar algumas libs de dados)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia o arquivo de dependências e instala as bibliotecas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- CONFIGURAÇÃO DO BIGQUERY ---
# Copia sua chave JSON da raiz do PC para a raiz do container
COPY google_credentials.json /app/google_credentials.json
# Define a variável de ambiente para que o Google SDK ache a chave
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/google_credentials.json"
# ------------------------------

# Copia o orquestrador e a pasta de código
COPY main.py . 
COPY src/ ./src/

# CRIAMOS AS PASTAS DE DADOS (Vazias, para o Python preencher depois)
RUN mkdir -p data/raw data/processed

# Avisa o Docker que o Streamlit usa a porta 8501
EXPOSE 8501

# O CMD padrão continua sendo o Streamlit
CMD ["streamlit", "run", "src/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]