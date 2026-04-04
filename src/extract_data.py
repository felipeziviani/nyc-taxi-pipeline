import kagglehub
import os

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data():
    data_dir = 'data/raw'
    os.makedirs(data_dir, exist_ok=True)

    logging.info("Iniciando etapa de extração...")
    if not os.listdir(data_dir):
        logging.info("Dataset não encontrado. Baixando do Kaggle...")
        
        path = kagglehub.dataset_download("elemento/nyc-yellow-taxi-trip-data")
        logging.info(f"Download concluído em: {path}")

        for file in os.listdir(path):
            src = os.path.join(path, file)
            dst = os.path.join(data_dir, file)
            os.rename(src, dst)
        logging.info("Arquivo movidos para data/raw...")
    else:
        logging.info("Dataset já existe. Pulando download.")
    files = os.listdir(data_dir)
    logging.info(f"{len(files)} arquivos disponíveis")  

    return data_dir