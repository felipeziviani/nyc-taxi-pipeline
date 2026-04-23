# Usa uma imagem leve do Python
FROM python:3.12-slim

# Instala dependências do sistema para o XGBoost e Google Cloud
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# Define a pasta de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de requisitos primeiro (otimiza o cache do Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código e o modelo treinado
COPY . .

# Expõe a porta que o Streamlit usa
EXPOSE 8501

# Comando para rodar o dashboard
CMD ["streamlit", "run", "src/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]