# Streaming Sensor Data Contract

## Purpose

This document defines the structure and validation requirements for sensor measurement events produced by the Python sensor simulator and consumed by the streaming data pipeline.

The contract is based on the BattLeDIM 2018 SCADA datasets investigated during Phase 2 and provides a consistent event structure for pressure, flow, tank-level, and demand measurements.

## Sensor Event Schema

Each sensor measurement is represented as an individual event.

Example:

```json
{
  "event_id": "pressure_n1_20180101T000000",
  "event_time": "2018-01-01T00:00:00",
  "sensor_id": "n1",
  "sensor_type": "pressure",
  "value": 28.42,
  "unit": "m"
}
```

### Event Fields

| Field | Description |
|---|---|
| `event_id` | Unique identifier for the sensor measurement event |
| `event_time` | Timestamp when the measurement occurred |
| `sensor_id` | Identifier of the sensor or monitored network asset |
| `sensor_type` | Type of measurement represented by the event |
| `value` | Numeric sensor measurement |
| `unit` | Unit associated with the measurement |

### Event ID Format

The event identifier follows this structure:

```text
<sensor_type>_<sensor_id>_<timestamp>
```

Example:

```text
pressure_n1_20180101T000000
```

Including `sensor_type` prevents identifier collisions when the same network identifier appears in more than one measurement type.

## Supported Sensor Types

| Sensor Type | Unit |
|---|---|
| `pressure` | m |
| `flow` | m³/h |
| `level` | m |
| `demand` | L/h |

The sensor types and units correspond to the BattLeDIM 2018 SCADA datasets used as the source for the simulated sensor stream.

## Event Validation Rules

A valid sensor event must:

- contain all required fields;
- contain a unique, non-empty `event_id`;
- contain a valid `event_time`;
- contain a non-empty `sensor_id`;
- use one of the supported `sensor_type` values;
- contain a numeric `value`;
- use the expected `unit` for its sensor type.

Structural validity and anomaly detection are treated as separate concerns.

A measurement may therefore be structurally valid while still representing unusual network behaviour that requires further analysis.

## Asset Metadata

Network and geographic information will be maintained separately from the sensor measurement events.

Asset metadata may include:

- network asset identifier;
- network asset type;
- sensor-to-asset relationship;
- network coordinates;
- network topology information.

Sensor events will be associated with network assets through their identifiers when spatial or network context is required.

This avoids repeatedly embedding relatively static network information inside every sensor event.

## Leakage Ground Truth

BattLeDIM leakage information will be maintained separately from the operational sensor stream.

Leakage ground truth includes information such as:

- affected link identifier;
- official leakage start time;
- leakage end time;
- leakage type;
- leakage peak time;
- leakage diameter.

These labels will be used later to evaluate anomaly-detection results.

Leakage labels will not be included in the normal sensor events supplied to the detection pipeline because the detection process should identify abnormal behaviour from operational measurements rather than receive the known leakage condition as an input.

## Contract Scope

This contract defines the initial measurement-event interface between the sensor simulator and the streaming pipeline.

Implementation-specific decisions such as Kafka topics, partitioning, serialization formats, Spark schemas, and storage schemas will be defined in the phases where those components are implemented.