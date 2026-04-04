from google.cloud import bigquery
import os

def test_bq_connection(): 
    try:
        client = bigquery.Client()
        datasets = list(client.list_datasets())
        if datasets:
            print("✅ Sucesso! Conectado ao BigQuery. Datasets encontrados:")
            for ds in datasets:
                print(f" - {ds.dataset_id}")
        else:
            print("✅ Conectado, mas nenhum dataset encontrado no projeto.")
    except Exception as e:
        print(f"❌ Erro ao conectar ao BigQuery: {e}")

if __name__ == "__main__":
    test_bq_connection()
