# BattLeDIM Data Understanding

## Purpose

This document summarizes the findings from the data acquisition and understanding phase of the Smart Water Network Intelligence Platform.

The objective of this phase was to understand the BattLeDIM source data, sensor measurements, leakage ground truth, data quality, and network topology before developing the streaming sensor simulator.

## Data Source

The project uses the BattLeDIM water-distribution benchmark dataset and the L-Town EPANET network model.

The 2018 historical dataset is used as the initial source for simulated sensor events.

The raw source files are preserved unchanged under:

`data/raw/battledim/`

Raw data is excluded from Git version control.

## SCADA Measurements

The 2018 SCADA datasets contain measurements recorded at 5-minute intervals throughout the year.

Each dataset contains 105,120 timestamps, covering:

`2018-01-01 00:00` to `2018-12-31 23:55`

The measurement sources are:

| Measurement | Sensors | Unit |
|---|---:|---|
| Pressure | 33 | m |
| Flow | 3 | m³/h |
| Tank level | 1 | m |
| Demand | 82 | L/h |

The source CSV files use a semicolon (`;`) delimiter and comma decimal separator.

Sensor behaviour differs substantially across assets. Pressure levels vary by network location, pump flow includes normal on/off operating states, tank level follows recurring filling and draining cycles, and demand measurements show substantial temporal variation.

These differences indicate that future anomaly detection should consider sensor and network context rather than rely on a single universal measurement threshold.

## Leakage Ground Truth

The 2018 leakage time series contains 14 affected network links.

BattLeDIM provides leakage metadata including:

- affected link;
- official start time;
- end time;
- leakage type;
- peak time;
- leakage diameter.

The 2018 events include both `incipient` and `abrupt` leakages.

Incipient leaks develop gradually, meaning their official start time may occur before a positive leakage flow is observed in the sampled time series.

Abrupt leaks begin immediately and, in the observed 2018 events, their official start, first positive measurement, and peak time coincide.

Four leakage events continue beyond the end of the 2018 historical dataset.

Leakage information will be maintained separately from operational sensor events and used as ground truth for later evaluation of anomaly-detection methods.

## Data Quality

The four 2018 SCADA datasets were assessed for:

- missing timestamps;
- duplicate timestamps;
- unexpected measurement intervals;
- missing measurements;
- negative measurements.

No issues were identified in these checks.

All four datasets contain a complete and consistent 5-minute measurement grid throughout 2018.

The source data will therefore be preserved rather than modified through unnecessary cleaning.

The future streaming pipeline will still validate incoming events because operational data-quality controls remain necessary even when the historical source dataset is clean.

## Network Topology

The L-Town EPANET model contains:

| Asset | Count |
|---|---:|
| Junctions | 782 |
| Reservoirs | 2 |
| Tanks | 1 |
| Pipes | 905 |
| Pumps | 1 |
| Valves | 3 |
| Node coordinates | 785 |

The 785 coordinates correspond to the 782 junctions, two reservoirs, and one tank.

Asset relationship validation confirmed:

- 33 of 33 pressure sensor IDs map to network nodes;
- 3 of 3 flow sensor IDs map to network links;
- 1 of 1 level sensor IDs maps to a network node;
- 14 of 14 leakage IDs map to network links.

This establishes a consistent relationship between SCADA measurements, network assets, leakage ground truth, and spatial information.

The nominal `L-TOWN.inp` model is sufficient for the current topology analysis because all required sensor and leakage identifiers map successfully to the network.

## Phase 2 Outcome

Phase 2 confirmed that the BattLeDIM 2018 dataset is suitable as the historical source for the Smart Water Network Intelligence Platform.

The project now has:

- understood and validated SCADA measurement data;
- identified leakage ground truth for later evaluation;
- verified source-data quality;
- established relationships between sensors, leakages, and physical network assets;
- confirmed availability of spatial network coordinates;
- defined the initial streaming sensor data contract.

The next phase will transform the historical SCADA measurements into individual sensor events and replay them through a Python sensor simulator.