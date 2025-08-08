import csv
from generated import energy_pb2

INPUT_CSV = "data/energy_data.csv"
OUTPUT_PB = "data/output1.pb"

def generate_raw_data():
    raw_data = []

    with open(INPUT_CSV, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = energy_pb2.RawEnergyData()
            record.timestamp = row["timestamp"]
            record.household_id = row["household_id"]
            record.power_consumption = row["power_consumption"]
            record.voltage = row["voltage"]
            record.current = row["current"]
            raw_data.append(record)

    # ✅ Wrap the raw records into RawDataReport
    report = energy_pb2.RawDataReport()
    report.raw_data.extend(raw_data)

    # ✅ Write RawDataReport to .pb file
    with open(OUTPUT_PB, "wb") as out:
        out.write(report.SerializeToString())

    print(f"✅ Wrote {len(raw_data)} records to {OUTPUT_PB}")

if __name__ == "__main__":
    generate_raw_data()
