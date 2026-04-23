from google.cloud import bigquery
import xgboost as xgb
from sklearn.model_selection import train_test_split
import joblib
import logging
from pathlib import Path
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
REAL_CREDENTIALS_PATH = BASE_DIR / "google_credentials.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(REAL_CREDENTIALS_PATH)
os.environ["GOOGLE_CLOUD_PROJECT"] = "heroic-bucksaw-492321-m3"

client = bigquery.Client()


def train_demand_model(processed_files):
    logging.info("➝ Iniciando treinamento do modelo de demanda...")

    query = """
    SELECT 
        EXTRACT(HOUR FROM tpep_pickup_datetime) as hora,
        EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) as dia_semana,
        # Criamos a zona arredondando Lat/Long (aprox 1km de precisão)
        # Isso gera um identificador numérico para a região
        CAST(ROUND(pickup_latitude, 2) * 100 + ROUND(pickup_longitude, 2) * 1000 AS INT64) as zona,
        count(*) as demanda
    FROM `heroic-bucksaw-492321-m3.nyc_taxi_data.yellow_cab_trips`
    WHERE pickup_latitude IS NOT NULL 
      AND pickup_latitude != 0
      AND tpep_pickup_datetime IS NOT NULL
    GROUP BY 1, 2, 3
    HAVING demanda > 5
    LIMIT 100000
    """

    logging.info(
        "➝ Executando consulta SQL para obter dados de treinamento...")
    df = client.query(query).to_dataframe()
    logging.info(f"➝ Dados de treinamento obtidos: {len(df)} registros")

    x = df[['hora', 'dia_semana', 'zona']]
    y = df['demanda']

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42)
    logging.info("➝ Dados divididos em treino e teste")
    model = xgb.XGBRegressor(n_estimators=100, max_depth=6, learning_rate=0.1)
    model.fit(x_train, y_train)
    logging.info("➝ Modelo treinado com sucesso")

    joblib.dump(model, 'models/modelo_taxi.joblib')
    logging.info("➝ Modelo salvo em 'models/modelo_taxi.joblib'")
    return model


if __name__ == "__main__":
    train_demand_model(None)
