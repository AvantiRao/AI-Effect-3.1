# Energy Analyzer Service

Processes raw energy data and generates derived metrics with anomaly detection.

## Purpose

Converts raw energy readings into processed reports with efficiency calculations, status indicators, and anomaly flags.

## Running the Service

```bash
# With Docker
docker-compose up analyzer

# Direct Python execution
python -m src.energy_analyzer.server
```

## API Endpoints

- Port: 50052
- Service: `EnergyAnalyzer`
- Method: `AnalyzeData(data: RawEnergyData[]) -> AnalyzeResponse`

## Processing Logic

1. Power Calculation: Converts string power to float
2. Efficiency: Calculates efficiency ratio (power / 150.0)
3. Status: Sets status based on efficiency threshold
4. Anomaly Detection: Flags high consumption as anomalies

## Sample Output

```json
{
  "report": {
    "processed": [
      {
        "timestamp": "2025-01-01T00:00:00Z",
        "household_id": "HH-0",
        "power": 120.0,
        "efficiency": 0.8,
        "status": "OK",
        "anomaly_detected": false
      }
    ],
    "skipped_rows": 0
  }
}
```