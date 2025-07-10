import csv
from generated import energy_pb2

INPUT_PB = "data/output2.pb"
OUTPUT_CSV = "data/energy_report.csv"

def main():
    # ✅ Load EnergyReport protobuf
    with open(INPUT_PB, "rb") as f:
        report = energy_pb2.EnergyReport()
        report.ParseFromString(f.read())

    processed = report.processed

    # ✅ Write to CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "household_id", "power", "efficiency", "status", "anomaly_detected"])
        for record in processed:
            writer.writerow([
                record.timestamp,
                record.household_id,
                record.power,
                record.efficiency,
                record.status,
                record.anomaly_detected
            ])

    print(f"✅ Report written to {OUTPUT_CSV} with {len(processed)} records, skipped: {report.skipped_rows}")

if __name__ == "__main__":
    main()
