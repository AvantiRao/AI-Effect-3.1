# Report Generator Service

Converts processed energy data into CSV reports for analysis and storage.

## Purpose

Takes analyzed energy reports and generates structured CSV files with all processed metrics and anomaly flags.

## Running the Service

```bash
# With Docker
docker-compose up reporter

# Direct Python execution
python -m src.report_generator.server
```

## API Endpoints

- Port: 50053
- Service: `ReportGenerator`
- Method: `GenerateReport(report: ProcessedDataReport) -> ReportResponse`

## Output Files

- Location: `/app/data/energy_report.csv` (container) or `./data/energy_report.csv` (host)
- Format: CSV with headers: timestamp, household_id, power, efficiency, status, anomaly_detected

## Sample CSV Output

```csv
timestamp,household_id,power,efficiency,status,anomaly_detected
2025-01-01T00:00:00Z,HH-0,120.0,0.8,OK,false
2025-01-01T00:00:01Z,HH-1,121.0,0.8066666666666666,OK,false
```