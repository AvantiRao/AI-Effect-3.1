from generated import energy_pb2

INPUT_PB = "data/output1.pb"
OUTPUT_PB = "data/output2.pb"

def analyze(record):
    try:
        if not record.timestamp or not record.household_id or \
           not record.power_consumption or not record.voltage or not record.current:
            raise ValueError("Missing required field")
            
        power = float(record.power_consumption)
        voltage = float(record.voltage)
        current = float(record.current)

        efficiency = round((power / (voltage * current)) * 100, 3)
        status = "high_usage" if power > 5.0 else "normal"
        anomaly = power > 5.0

        processed = energy_pb2.ProcessedEnergyReport(
            timestamp=record.timestamp,
            household_id=record.household_id,
            power=power,
            efficiency=efficiency,
            status=status,
            anomaly_detected=anomaly
        )
        return processed, True
    except Exception:
        return None, False


def main():
    # ✅ Read the RawDataReport message from .pb file
    with open(INPUT_PB, "rb") as f:
        report = energy_pb2.RawDataReport()
        report.ParseFromString(f.read())
        raw_records = report.raw_data

    processed_records = []
    skipped = 0

    # ✅ Analyze each raw record
    for record in raw_records:
        result, valid = analyze(record)
        if valid:
            processed_records.append(result)
        else:
            skipped += 1

    # ✅ Write ProcessedEnergyReport list into a ProcessedDataReport message
    output_report = energy_pb2.ProcessedDataReport()
    output_report.processed.extend(processed_records)
    output_report.skipped_rows = skipped

    with open(OUTPUT_PB, "wb") as f:
        f.write(output_report.SerializeToString())

    print(f"Processed {len(processed_records)} records, skipped {skipped}")


if __name__ == "__main__":
    main()
