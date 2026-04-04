from src.extract_data import extract_data
from src.tranform_data import transform_data
from src.aggregate_data import aggregate_data

if __name__ == "__main__":
    path = extract_data()
    processed_files  = transform_data(path)
    results = aggregate_data(processed_files)