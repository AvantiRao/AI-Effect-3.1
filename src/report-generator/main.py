import json, csv, os

INPUT_FILE = "../../data/output2.json"
OUTPUT_FILE = "../../data/energy_report.csv"

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r") as infile:
        data = json.load(infile)

    fieldnames = ["timestamp", "household_id", "power", "efficiency", "status"]
    with open(OUTPUT_FILE, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({
                "timestamp": row["timestamp"],
                "household_id": row["household_id"],
                "power": row["power"],
                "efficiency": row["efficiency"],
                "status": row["status"]
            })
    print(f" Container 3: Report generated with {len(data)} records.")

if __name__ == "__main__":
    main()