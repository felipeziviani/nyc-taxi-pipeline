from src.extract_data import extract_data
from src.tranform_data import transform_data
from src.ml_engine import train_demand_model
from src.aggregate_data import aggregate_data

if __name__ == "__main__":
    path = extract_data()
    processed_files  = transform_data(path)
    results = aggregate_data(processed_files)
    model = train_demand_model(processed_files)