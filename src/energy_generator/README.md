# Energy Generator Service

Generates synthetic household energy data for testing and development.

## Purpose

Creates realistic energy consumption records with timestamps, household IDs, power consumption, voltage, and current values.

## Running the Service

```bash
# With Docker
docker-compose up generator

# Direct Python execution
python -m src.energy_generator.server
```

## API Endpoints

- Port: 50051
- Service: `EnergyGenerator`
- Method: `GenerateData(rows: int) -> GenerateResponse`

## Sample Output

```json
{
  "data": [
    {
      "timestamp": "2025-01-01T00:00:00Z",
      "household_id": "HH-0",
      "power_consumption": "120",
      "voltage": "230",
      "current": "5"
    }
  ]
}
```