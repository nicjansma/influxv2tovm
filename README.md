## Migrate data from InfluxDB v2 to VictoriaMetrics with Home Assistant support
This project provides a Python script to import data from InfluxDB >=2.0 to VictoriaMetrics and is highly inspired and developed originally from
https://github.com/jonppe/influx_to_victoriametrics/ and then significant extra work, including [Home Assistant](https://www.home-assistant.io) support, from Fredrik J-L at https://github.com/frli4797/influxv2tovm/

The [command line tool](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/docs/vmctl.md) packaged with VictoriaMetrics provides similar features for InfluxDB 1.X, although without specific support for Home Assistant.

Every unique time series is queried one by one and exported to VictoriaMetrics. For large datasets the metrics are broken into chunks (100,000 rows by default, set with `--chunk-size`) and submitted in batches using either the legacy Influx [Line Protocol API](https://archive.docs.influxdata.com/influxdb/v0.9/write_protocols/write_syntax/) (the default) or, for Home Assistant data, the [Prometheus exposition format](https://docs.victoriametrics.com/#how-to-import-data-in-prometheus-exposition-format) using the same metric names as the Home Assistant [Prometheus integration](https://www.home-assistant.io/integrations/prometheus/).

## About this fork
This project is a fork based on the [fri4797](https://github.com/frli4797/influxv2tovm) fork of the original project developed by [jonppe](https://github.com/jonppe/influx_to_victoriametrics/). The initial project was designed to migrate multiple InfluxDB v2 buckets into a single VictoriaMetrics DB each annotated with a key designating the DB source for delineation. The limitations of that version is that the migration was done by entire metric and in a sufficiently large dataset a single metric could exceed the resources of the runtime.

The original project was forked by Fredrick J-L in summer 2024 where it was substantially reworked to add chunking of the data into controlled transaction sizes and support for InfluxDBs populated with [Home Assistant](https://www.home-assistant.io) data. Home Assistant (HA) is a home automation system that generates many data points stored in a SQL DB and it is not unusual for deployers to supplement the SQL DB with a time series DB for historical data, usually InfluxDB v1 or v2.

As Influx has pivoted to enterprise customers, many HA users have been inspired by the [VictoriaMetrics Add-on](https://github.com/fuslwusl/homeassistant-addon-victoriametrics) developed by fuslwusl to use VictoriaMetrics (VM) as it more closely matched the use case. The HA integration is highly configurable and other integrations can provide metrics leading to a highly diverse dataset.

This project uses the legacy Influx [Line Protocol API](https://archive.docs.influxdata.com/influxdb/v0.9/write_protocols/write_syntax/) to submit metrics to VM and the heterogenous nature of HA data sources exposed a number of flaws in the original project that did not sanitize the input data and created API calls that did not comply with the ingest specification.


## Usage
~~~~
./influxv2tovm.py -h
usage: influxv2tovm.py [-h] [--INFLUXDB_V2_ORG INFLUXDB_V2_ORG] [--INFLUXDB_V2_URL INFLUXDB_V2_URL]
                       [--INFLUXDB_V2_TOKEN INFLUXDB_V2_TOKEN] [--INFLUXDB_V2_SSL_CA_CERT INFLUXDB_V2_SSL_CA_CERT]
                       [--INFLUXDB_V2_TIMEOUT INFLUXDB_V2_TIMEOUT] [--INFLUXDB_V2_VERIFY_SSL INFLUXDB_V2_VERIFY_SSL]
                       [--VM_ADDR VM_ADDR] [--dry-run] [--pivot] [--output-format {influx,prometheus}]
                       [--prometheus-namespace PROMETHEUS_NAMESPACE] [--fahrenheit] [--job JOB] [--max-retries MAX_RETRIES]
                       [--skip-existing] [--entity-id ENTITY_ID] [--debug]
                       [--history-window HISTORY_WINDOW] [--metric-search-window METRIC_SEARCH_WINDOW]
                       [--chunk-size CHUNK_SIZE]
                       bucket

Script for exporting InfluxDB data into victoria metrics instance. InfluxDB settings can be defined on command line or
as environment variables (or in .env file if python-dotenv is installed). InfluxDB related args described in
https://github.com/influxdata/influxdb-client-python#via-environment-properties

positional arguments:
  bucket                InfluxDB source bucket

options:
  -h, --help            show this help message and exit
  --INFLUXDB_V2_ORG INFLUXDB_V2_ORG, -o INFLUXDB_V2_ORG
                        InfluxDB organization
  --INFLUXDB_V2_URL INFLUXDB_V2_URL, -u INFLUXDB_V2_URL
                        InfluxDB Server URL, e.g., http://localhost:8086
  --INFLUXDB_V2_TOKEN INFLUXDB_V2_TOKEN, -t INFLUXDB_V2_TOKEN
                        InfluxDB access token.
  --INFLUXDB_V2_SSL_CA_CERT INFLUXDB_V2_SSL_CA_CERT, -S INFLUXDB_V2_SSL_CA_CERT
                        Server SSL Cert
  --INFLUXDB_V2_TIMEOUT INFLUXDB_V2_TIMEOUT, -T INFLUXDB_V2_TIMEOUT
                        InfluxDB timeout
  --INFLUXDB_V2_VERIFY_SSL INFLUXDB_V2_VERIFY_SSL, -V INFLUXDB_V2_VERIFY_SSL
                        Verify SSL CERT
  --VM_ADDR VM_ADDR, -a VM_ADDR
                        VictoriaMetrics server URL, e.g., http://localhost:8428
  --dry-run, -n         Dry run
  --pivot, -P           Pivot entity_id to be measurement
  --output-format {influx,prometheus}, -f {influx,prometheus}
                        Format written to VictoriaMetrics. 'influx' copies the InfluxDB series as-is (line protocol).
                        'prometheus' converts Home Assistant data to the metric names and labels the Home Assistant
                        Prometheus integration uses, e.g.
                        homeassistant_sensor_battery_percent{domain=...,entity=...,friendly_name=...}. Default: influx
  --prometheus-namespace PROMETHEUS_NAMESPACE
                        Metric name prefix for --output-format prometheus, matching the Prometheus integration's
                        'namespace' setting. Pass an empty string for no prefix. Default: homeassistant
  --fahrenheit          For --output-format prometheus: Home Assistant uses Fahrenheit, so climate temperature
                        attributes are converted to Celsius like the Prometheus integration does
  --job JOB             For --output-format prometheus: add a job label with this value to every metric, matching the
                        job name of the scrape config that collects Home Assistant's Prometheus metrics
  --max-retries MAX_RETRIES
                        How many times to retry a failed request to VictoriaMetrics (connection errors, timeouts, HTTP
                        429 and 5xx), with exponential back-off capped at 60s between attempts. Default: 99
  --skip-existing       Skip any series that already has data in VictoriaMetrics, checked with its Prometheus series
                        API
  --entity-id ENTITY_ID, -e ENTITY_ID
                        Only migrate series with this entity_id. Repeat the flag or pass a comma-separated list (e.g.
                        -e sensor_a,sensor_b -e sensor_c)
  --debug, -d           Print extra diagnostic output, such as the list of discovered time series
  --history-window HISTORY_WINDOW
                        How far back to migrate, as an InfluxDB Flux duration (e.g. 100d, 730d, 2y). Default: 100d
  --metric-search-window METRIC_SEARCH_WINDOW
                        How far back to look when discovering which series to migrate, as an InfluxDB Flux duration
                        (e.g. 1d, 30d). Default: same as --history-window
  --chunk-size CHUNK_SIZE
                        Maximum rows read per InfluxDB field per query, and so roughly per write to VictoriaMetrics.
                        Larger chunks mean fewer queries but more memory. Default: 100000
~~~~

With the default `influx` output format, the bucket name is also added to the `db` tag in VictoriaMetrics.

### Choosing what to migrate
The migration runs in two steps: it first finds the unique series in the bucket, then copies each series in chunks. These options control which data is included:

* `--history-window` sets how far back data is copied, as a [Flux duration](https://docs.influxdata.com/flux/v0/spec/lexical-elements/#duration-literals) such as `100d`, `730d` or `2y`. The default is `100d`, so pass a larger window to copy your full history.
* `--metric-search-window` sets how far back the first step looks for series. It defaults to the history window. A shorter window such as `1d` makes discovery much faster on large buckets, but series with no data inside it are not migrated at all.
* `--entity-id` (`-e`) restricts the migration to specific Home Assistant entities. The value is the `entity_id` tag as Home Assistant writes it to InfluxDB, which is the entity ID without its domain (e.g. `garage_sensor_battery_level` for `sensor.garage_sensor_battery_level`). Repeat the flag or pass a comma-separated list.

### Resuming and retrying
Requests to VictoriaMetrics that fail with a connection error, timeout, HTTP 429 or HTTP 5xx are retried with exponential back-off (1s, 2s, 4s, ... up to 60s between attempts), up to `--max-retries` times (default 99). Other errors, such as VictoriaMetrics rejecting the data, are printed and logged to `migrator.log`, and the migration carries on.

`--skip-existing` checks VictoriaMetrics' Prometheus series API (`/api/v1/series`) before each series and skips any that already has data, which lets you resume an interrupted migration. It matches:

* with `--output-format prometheus`: the `entity` label (and `job`, when `--job` is set),
* with `--pivot`: the `entity_id` and `db` labels,
* otherwise: metric names starting with the measurement, plus the `db` label.

A series that was only partly written before an interruption also counts as existing, so it won't be completed. Delete it from VictoriaMetrics before resuming if you need it in full.

### Checking a run before writing
`--dry-run` (`-n`) does everything except writing to VictoriaMetrics. Adding `--debug` (`-d`) prints:

* the Flux queries sent to InfluxDB,
* every series found in the first step,
* with `--dry-run`, the exact lines that would be written to VictoriaMetrics.

A good first check is a dry run of a single entity:
~~~~
./influxv2tovm.py --pivot --dry-run --debug -e garage_sensor_battery_level home_assistant
~~~~

## Running in a Dev Container
It is likely that most people using this tool will use it once to migrate a data set and never again. The target audience is therefore likely not a developer, familiar with Python or even has a workable Python environment.

To simplify usage of this tool this repo includes a definition for a Dev Container. This is compatible with many developer environments including VSCode and Github Codespaces. On opening the workspace you will be prompted to reopen the project in a Dev Container.

This will download an Ubuntu container with Python pre-installed and then install all the project library dependencies before opening a terminal prompt. If using VSCode then the Python support extensions will be installed and there is a Debugger launch definition should you want to step through the code.

The python tool supports setting parameters from environment variables. If you need to run the tool multiple times it might be easier to define these parameters as environment variables by editing the `./devcontainer/.devcontainer.json` file. Alternatively set them in the .env file created at the root of the project when the dev container is started.

## Home Assistant migration
If the source bucket was populated by Home Assistant then it will need some specific transforming, specifically the _measurement column moved to a unit_of_measurement label and replaced with the Home Assistant EntityID.

It is recommended to run this tool in a Dev Container as it vastly simplifies configuring the Python runtime environment. While Dev Containers are widely supported such as Github Codespaces it is probable that your VM and Influx endpoints are not publically exposed so the best local tool would be [VSCode](https://code.visualstudio.com). See the instruction above on how to use a Dev Container.

If you are using the [VictoriaMetrics Addon](https://github.com/fuslwusl/homeassistant-addon-victoriametrics) then the VM_ADDR parameter or environment variable should be set to the url of Home Assistant on port 8428, e.g. `http://homeassistant.local:8428`. You will need to get the Influx URL, token and org from your InfluxDB v2 installation. Once your Dev Container is running there should be a terminal pane to run the following:
~~~~
./influxv2tovm.py --pivot --dry-run home_assistant
~~~~

If this execution looks good then simply re-run the above command omitting the `--dry-run` parameter.

### Prometheus output
If Home Assistant now sends data to VictoriaMetrics through the [Prometheus integration](https://www.home-assistant.io/integrations/prometheus/), the default output won't line up with it: the migrated history keeps InfluxDB's naming while new data uses Prometheus metric names. Use `--output-format prometheus` (`-f prometheus`) to convert the history into the integration's format instead, so old and new data end up in the same series:
~~~~
./influxv2tovm.py --output-format prometheus --dry-run --debug home_assistant
~~~~

This writes lines such as
~~~~
homeassistant_sensor_battery_percent{domain="sensor",entity="sensor.garage_sensor_battery_level",friendly_name="Garage Sensor Garage Sensor battery_level"} 100.0 1788264000000
~~~~
to VictoriaMetrics' `/api/v1/import/prometheus` endpoint. Metric names follow the integration's rules:

| Domain | Metrics |
|---|---|
| `sensor` | `sensor_<device_class>_<unit>`, `sensor_unit_<unit>` when there is no device class, or `sensor_state` when there is no unit. `°C`/`°F` become `celsius` (Fahrenheit values are converted) and `%` becomes `percent`. |
| `binary_sensor`, `switch`, `input_boolean`, `input_number`, `number`, `lock`, `fan`, `humidifier`, `device_tracker`, `person`, `update` | `<domain>_state`, or `<domain>_state_<unit>` when there is a unit |
| `counter` | `counter_value` |
| `light` | `light_brightness_percent` |
| `climate` | `climate_target_temperature_celsius` (plus `_high`/`_low`), `climate_current_temperature_celsius`, and `climate_action`, `climate_mode`, `climate_preset_mode`, `climate_fan_mode` (one series per value: 1 for the current one, 0 for the rest) |
| `cover` | `cover_state` (one series per state), `cover_position`, `cover_tilt_position` |

Every metric has the `domain`, `entity` and `friendly_name` labels. Other domains are skipped and listed in `migrator.log`.

Notes:
* Use `--prometheus-namespace` if you changed the integration's `namespace` setting. The default is `homeassistant`.
* A scrape adds a `job` label (and usually `instance`) to every metric. Pass `--job` with your scrape config's `job_name` so the migrated history lands in the same series as newly scraped data.
* Climate temperature attributes are stored in InfluxDB without a unit. If Home Assistant uses Fahrenheit, pass `--fahrenheit` so they are converted to Celsius like the integration does.
* The conversion expects Home Assistant's default InfluxDB schema: the unit as the measurement (or the entity ID when there is no unit), the state in the `value`/`state` fields, and attributes as fields, with a `_str` suffix for non-numeric values. If you set `default_measurement` in Home Assistant's InfluxDB config, entities without a unit will get the wrong metric name.
* The integration's `entity_available`, `last_updated_time_seconds` and `state_change` metrics can't be rebuilt from InfluxDB data, so they aren't migrated.
* `climate_mode`, `climate_preset_mode` and `climate_fan_mode` use the list of available modes stored with each state. If that list wasn't stored, only the current mode is written.

### Re-running after an earlier version
Earlier versions paged through each series incorrectly, so series with more than one field or tag set (which includes all Home Assistant data) were only partly copied. Re-running the migration with this version copies everything. To avoid duplicate points for the data that was already copied, delete those series from VictoriaMetrics first, or enable [deduplication](https://docs.victoriametrics.com/#deduplication).

---
Author: Max Lyth

Thanks to: Johannes Aalto & Fredrik J-L

SPDX-License-Identifier: Apache-2.0
