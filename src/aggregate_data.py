from google.cloud import bigquery

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def aggregate_data(): 
    logging.info("Iniciando etapa de agregação...")

    try:
        client = bigquery.Client()  
        query = """
            SELECT * FROM `heroic-bucksaw-492321-m3.nyc_taxi_data.yellow_cab_trips LIMIT 500000`
        """
        df = client.query(query, location='southamerica-east1').to_dataframe()
            
        metrics = {
            'total_trips': len(df),
            'avg_trip_distance': df['trip_distance'].mean(),
            'avg_fare': df['total_amount'].mean(),
            'avg_duration': df['trip_duration_min'].mean(),
            "total_revenue": df["total_amount"].sum(),
        }

        metrics = {k: float(v) for k, v in metrics.items()}

        df["avg_speed"] = df["trip_distance"] / df["trip_duration_min"]
        
        if "tip_amount" in df.columns:
            df["tip_pct"] = df["tip_amount"] / df["total_amount"]
            metrics["avg_tip_pct"] = df["tip_pct"].mean()
        logging.info(f"Métricas gerais: {metrics}")

        df["date"] = df["tpep_pickup_datetime"].dt.date

        daily = df.groupby("date").agg(
            total_trips=("trip_distance", "count"),
            total_revenue=("total_amount", "sum"),
            avg_fare=("total_amount", "mean"),
            avg_duration=("trip_duration_min", "mean")
        ).reset_index()

        period_payment = None
        if "payment_type" in df.columns:
            period_payment = df.groupby(["period", "payment_type"]).agg(
                total_trips=("trip_distance", "count"),
                total_revenue=("total_amount", "sum")
            ).reset_index()

        percentiles = {
            'p50_duration': df['trip_duration_min'].quantile(0.5),
            'p90_duration': df['trip_duration_min'].quantile(0.9),
            'p95_duration': df['trip_duration_min'].quantile(0.95),
        }

        percentiles = {k: float(v) for k, v in percentiles.items()} 
        logging.info(f"Percentis: {percentiles }")

        return {
            "metrics": metrics,
            "daily": daily,
            "period_payment": period_payment,
            "percentiles": percentiles
        }
    except Exception as e:
        logging.error(f"Erro durante agregação via BigQuery: {e}")
        return None