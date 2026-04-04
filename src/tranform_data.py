import pandas as pd
import os
import logging
import pandas_gbq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(data_dir):
    logging.info("Iniciando transformação por lotes (chunks)...")
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'heroic-bucksaw-492321-m3')
    dataset_id = os.getenv('DATASET_ID', 'nyc_taxi_data')
    table_name = os.getenv('TABLE_ID', 'yellow_cab_trips')
    table_id = f"{dataset_id}.{table_name}"
    is_first_chunk = True

    processed_files = []
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)

    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    for file in files:
        file_path = os.path.join(data_dir, file)
        logging.info(f"Processando arquivo: {file}")
        chunk_count = 0

        for chunk in pd.read_csv(file_path, chunksize=50000):
            chunk_count += 1
            chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'], errors="coerce")
            chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'], errors="coerce")
            
            chunk = chunk.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

            chunk['trip_duration_min'] = (
                chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime']
            ).dt.total_seconds() / 60

            temp_distance = chunk['trip_distance'].replace(0, pd.NA)
            temp_duration = chunk['trip_duration_min'].replace(0, pd.NA)

            chunk['revenue_per_min'] = chunk['total_amount'] / temp_duration
            chunk['revenue_per_mile'] = chunk['total_amount'] / temp_distance

            chunk['is_anomaly'] = (
                (chunk['revenue_per_mile'] < 1.5) & (chunk['trip_distance'] > 5) | 
                (chunk['trip_duration_min'] > 120) |                                            
                (chunk['total_amount'] > 150)                                                  
            )

            invalid_mask = (
                (chunk['trip_distance'] <= 0) | (chunk['trip_distance'] > 500) |
                (chunk['passenger_count'] < 0) | (chunk['passenger_count'] > 8) |
                (chunk['total_amount'] <= 0) |
                (chunk['trip_duration_min'] <= 0) | (chunk['trip_duration_min'] > 1440) |
                (chunk['pickup_latitude'] < 40) | (chunk['pickup_latitude'] > 42) |
                (chunk['pickup_longitude'] < -75) | (chunk['pickup_longitude'] > -72) |
                (chunk['dropoff_latitude'] < 40) | (chunk['dropoff_latitude'] > 42) |
                (chunk['dropoff_longitude'] < -75) | (chunk['dropoff_longitude'] > -72)
            )

            chunk_clean = chunk[~invalid_mask].copy()

            logging.info(f"Registros originais: {len(chunk)}")
            logging.info(f"Registros válidos: {len(chunk_clean)}")
            logging.info(f"Removidos (erros/geografia): {len(chunk) - len(chunk_clean)}")

            chunk_clean = chunk_clean.drop_duplicates(
                subset=['tpep_pickup_datetime', 'trip_distance', 'total_amount']
            )

            chunk_clean['distance_category'] = pd.cut(
                chunk_clean['trip_distance'],
                bins=[0, 2, 10, 500],
                labels=['curta', 'media', 'longa']
            )

            chunk_clean['duration_category'] = pd.cut(
                chunk_clean['trip_duration_min'],
                bins=[0, 5, 30, 1440],
                labels=['muito_curta', 'normal', 'longa']
            )

            chunk_clean['hour'] = chunk_clean['tpep_pickup_datetime'].dt.hour

            def get_period(hour):
                if 5 <= hour < 12: return 'manhã'
                elif 12 <= hour < 17: return 'tarde'
                elif 17 <= hour < 21: return 'noite'
                else: return 'madrugada'
                
            chunk_clean['period'] = chunk_clean['hour'].apply(get_period)
            chunk_clean['is_weekend'] = chunk_clean['tpep_pickup_datetime'].dt.weekday >= 5

            output_file = os.path.join(output_dir, f"proc_{chunk_count}_{file}".replace('.csv', '.parquet'))
            chunk_clean.to_parquet(output_file, index=False)
            processed_files.append(output_file)
            logging.info(f"Chunk {chunk_count} salvo: {output_file}")

            try:
                mode = 'replace' if is_first_chunk else 'append'
                logging.info(f"Enviando para BigQuery: {table_id}")
                pandas_gbq.to_gbq(
                    chunk_clean,
                    table_id,
                    project_id=project_id,
                    if_exists=mode,
                    progress_bar=False
                )
                is_first_chunk = False
                logging.info(f"Lote {chunk_count} enviado via {mode}")
            except Exception as e:
                logging.error(f"Erro ao enviar para BigQuery: {e}")
            
    logging.info("Transformação e Carga concluídas com sucesso!")
    return processed_files