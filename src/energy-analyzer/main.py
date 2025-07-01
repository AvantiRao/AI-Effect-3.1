import json, os

INPUT_FILE = '../../data/output1.json'
OUTPUT_FILE = '../../data/output2.json'
DATA_FIELDS = ["timestamp", "household_id", "power_consumption", "voltage", "current"]

# Check fields and missing values if any
def validate_row(row):
    for field in DATA_FIELDS:
        if field not in row or row[field] in [None, "", "null"]:
            raise ValueError(f"Invalid or missing '{field}' in row: {row}")

# Data Analysis-Logic
def process_row(row):
    power, voltage, current = map(float, (row["power_consumption"], row["voltage"], row["current"]))
    return {
        "timestamp": row["timestamp"],
        "household_id": row["household_id"],
        "power": power,
        "efficiency": round((power / (voltage * current)) * 100, 3),
        "status": "high_usage" if power > 5.0 else "normal",
        "anomaly_detected": power > 5.0
    }

#Processed data to output JSON
def write_json(data, path):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

#Main function
def main():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f" Input file not found: {INPUT_FILE}")
    with open(INPUT_FILE) as infile:
        raw_data = json.load(infile)

    processed_data = []

    for row in raw_data:
        try:
            validate_row(row)             # 1: Check for valid structure
            processed = process_row(row)  # 2: Process logic
            processed_data.append(processed)
        except Exception as e:
             raise ValueError(f"Invalid or missing data")


    write_json(processed_data, OUTPUT_FILE)  # 3 Save to JSON file - output2.json
    print(json.dumps(processed_data, indent=2))
    print(f"Container 2: Processed {len(processed_data)} records")
    
if __name__ == "__main__":
    main()