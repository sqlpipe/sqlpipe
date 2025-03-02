import sys
import json
import pandas as pd
from presidio_structured import PandasAnalysisBuilder

def analyze_csv(csv_path, custom_strategy_threshold, custom_strategy_percentile, num_rows_to_scan_for_pii=100):
    # Load CSV
    df = pd.read_csv(csv_path)
    df = df.astype(str)

    entities_whitelist = [
        'CREDIT_CARD',
        'CRYPTO',
        'EMAIL_ADDRESS',
        'IBAN_CODE',
        'IP_ADDRESS',
        'NRP',
        'LOCATION',
        'PERSON',
        'PHONE_NUMBER',
        'MEDICAL_LICENSE',
        'URL',
        'US_BANK_NUMBER',
        'US_DRIVER_LICENSE',
        'US_ITIN',
        'US_PASSPORT',
        'US_SSN'
    ]
    

    result = PandasAnalysisBuilder().generate_analysis(df=df, n=num_rows_to_scan_for_pii, selection_strategy='custom', custom_strategy_threshold=custom_strategy_threshold, custom_strategy_percentile=custom_strategy_percentile)

    keys_to_remove = [entity for entity in result.entity_mapping if result.entity_mapping[entity] not in entities_whitelist]
    for entity in keys_to_remove:
        del result.entity_mapping[entity]
    
    return result.entity_mapping

if __name__ == "__main__":
    csv_path = sys.argv[1]
    custom_strategy_threshold = float(sys.argv[2])
    custom_strategy_percentile = float(sys.argv[3])
    num_rows_to_scan_for_pii = int(sys.argv[4])
    
    result = analyze_csv(csv_path, custom_strategy_threshold, custom_strategy_percentile, num_rows_to_scan_for_pii)
    print(json.dumps(result))
