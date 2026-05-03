# Telemetry

Structured logs from brew sessions. Every sensor read, pump toggle, and timer event is timestamped.

## Format

Events are logged as structured JSON:

```json
{
  "ts": "2026-02-21T08:15:22.041Z",
  "source": "ble_g30",
  "event": "temp_read",
  "data": {
    "current_c": 64.2,
    "target_c": 65.0,
    "heater": true,
    "pump": true
  }
}
```

## Data from Batch #001

Session telemetry from the February 21 brew day will be published here as the reference dataset — a complete, reproducible record of an AI-managed brew session.
