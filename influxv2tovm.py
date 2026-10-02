#!/usr/bin/env python3
"""
 @author Fredrik Lilja

 SPDX-License-Identifier: Apache-2.0
"""
import ast
import datetime
import json
import logging
import math
import os
import re
import string
import time
import warnings
from typing import Iterable, Dict, List, Optional

import humanize
import pandas as pd
import requests
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction)

# Create a custom logger
logger = logging.getLogger(__name__)
# noinspection SpellCheckingInspection
logging.basicConfig(filename="migrator.log", encoding="utf-8", level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s -  %(message)s')

try:
    # noinspection PyUnresolvedReferences
    import dotenv

    dotenv.load_dotenv(dotenv_path=".env")
except ImportError as err:
    pass


class Stats:
    bytes: int = 0
    lines: int = 0

    def humanized_bytes(self) -> str:
        """
        Get the number of bytes as natural size.
        :return: str
        """
        return humanize.naturalsize(self.bytes)

    def increment(self, lines: str):
        """
        Increments the number of bytes and the number of lines from a string.
        :param lines: lines string
        """
        no_lines = lines.count('\n')
        self.lines = self.lines + no_lines

        new_bytes = len(lines.encode("utf8"))
        self.bytes += new_bytes


# Mirrors the metric naming of the Home Assistant Prometheus integration:
# https://github.com/home-assistant/core/blob/dev/homeassistant/components/prometheus/__init__.py
PROMETHEUS_ALLOWED_METRIC_CHARS = set(string.ascii_letters + string.digits + "_:")
# Domains the Prometheus integration exports as "<domain>_state" (or "<domain>_state_<unit>")
PROMETHEUS_NUMERIC_STATE_DOMAINS = {
    "binary_sensor", "input_boolean", "input_number", "number", "device_tracker", "person",
    "lock", "update", "humidifier", "fan", "switch",
}

# InfluxDB fields read in Prometheus mode. Home Assistant writes the state as "value" (numeric) and/or "state"
# (string), numeric attributes under their own name, and other attributes with a "_str" suffix.
PROMETHEUS_FIELDS = [
    "value", "state", "device_class_str", "friendly_name_str",
    # light
    "brightness",
    # climate
    "temperature", "target_temp_high", "target_temp_low", "current_temperature",
    "hvac_action_str", "hvac_modes_str", "preset_mode_str", "preset_modes_str", "fan_mode_str", "fan_modes_str",
    # cover
    "current_position", "current_tilt_position",
]
PROMETHEUS_HVAC_ACTIONS = ["cooling", "defrosting", "drying", "fan", "heating", "idle", "off", "preheating"]
PROMETHEUS_COVER_STATES = ["closed", "closing", "open", "opening"]


def prometheus_unit_string(unit: Optional[str]) -> Optional[str]:
    """
    Converts a Home Assistant unit of measurement into the unit suffix used by the Prometheus integration.
    """
    if not unit:
        return None
    units = {"°C": "celsius", "°F": "celsius", "%": "percent"}
    default = unit.replace("/", "_per_").replace("\u03bc", "\u00b5").lower()
    return units.get(unit, default)


def prometheus_sanitize_metric_name(metric: str) -> str:
    """
    Replaces characters not allowed in Prometheus metric names, the same way the Prometheus integration does.
    """
    metric = metric.replace("\u03bc", "\u00b5")
    return "".join(c if c in PROMETHEUS_ALLOWED_METRIC_CHARS else f"u{hex(ord(c))}" for c in metric)


def prometheus_escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")


def promql_string(value: str) -> str:
    """
    Quotes a value as a PromQL string literal.
    """
    return json.dumps(value, ensure_ascii=False)


def is_missing(value) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def to_float(value) -> Optional[float]:
    """
    Converts a field value to a float, or None if it is missing or not numeric.
    """
    if is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_list_attribute(value) -> Optional[List[str]]:
    """
    Parses a list attribute (e.g. hvac_modes) that Home Assistant stored in InfluxDB as a string like "['off', 'heat']".
    """
    if is_missing(value):
        return None
    for parse in (ast.literal_eval, json.loads):
        try:
            parsed = parse(str(value))
        except (ValueError, SyntaxError):
            continue
        if isinstance(parsed, (list, tuple)):
            return [str(v) for v in parsed]
    return None


def prometheus_enum_samples(metric: str, label: str, current: Optional[str], candidates: Optional[List[str]]) \
        -> List[tuple]:
    """
    One sample per candidate, 1.0 for the current value and 0.0 for the rest, like the Prometheus integration's
    enum gauges. Falls back to just the current value when the list of candidates wasn't stored.
    """
    if is_missing(current):
        return []
    current = str(current)
    if not candidates:
        candidates = [current]
    return [(metric, {label: candidate}, 1.0 if candidate == current else 0.0) for candidate in candidates]


class InfluxMigrator:
    __query_api: QueryApi
    __measurement_key = "_measurement"
    __client: InfluxDBClient

    # noinspection SpellCheckingInspection
    def __init__(self, bucket: str, vm_url: str, chunksize: int = 100, dry_run: bool = False, pivot: bool = False,
                 history_window: str = "100d", metric_search_window: str = "100d",
                 debug: bool = False, entity_ids: Optional[List[str]] = None, output_format: str = "influx",
                 prometheus_namespace: str = "homeassistant", fahrenheit: bool = False,
                 job: Optional[str] = None, max_retries: int = 99, skip_existing: bool = False):
        self.bucket = bucket
        self.vm_url: str = vm_url
        self.chunksize = chunksize
        self.history_window = history_window
        self.metric_search_window = metric_search_window
        # now_datetime_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # self.__progress_file = open(f".migrator_{now_datetime_str}", 'w')
        self.stats = Stats()
        self.dry_run = dry_run
        self.pivot = pivot
        self.debug = debug
        self.entity_ids = entity_ids or []
        self.output_format = output_format
        self.prometheus_prefix = f"{prometheus_namespace}_" if prometheus_namespace else ""
        self.fahrenheit = fahrenheit
        self.job = job
        self.max_retries = max_retries
        self.skip_existing = skip_existing
        # Prometheus output converts Home Assistant entities one at a time, so it always looks series up by entity_id
        if pivot or output_format == "prometheus":
            self.__measurement_key = "entity_id"
        else:
            self.__measurement_key = "_measurement"

    def __del__(self):
        self.__client.close()

    def influx_connect(self):
        """
        Connects to the influx database.
        """
        self.__client = InfluxDBClient.from_env_properties()
        self.__query_api = self.__client.query_api()

    def migrate(self):

        if self.__query_api is None:
            raise AssertionError("No connection to InfluxDb started.")

        # Get all unique series by reading first entry of every table.
        # With latest InfluxDB we could possibly use "schema.measurements()" but this doesn't exist in 2.0
        measurements_and_fields = self.__find_all_measurements()

        field_no = 1
        for meas in measurements_and_fields:
            no_lines = 0

            if self.skip_existing and self.__series_exists(meas):
                print(f"Skipping {meas}, it already exists in VictoriaMetrics "
                      f"({field_no}/{len(measurements_and_fields)})")
                field_no += 1
                continue

            # Page through the series by time: each query reads at most chunksize rows per table starting at
            # the cursor, which InfluxDB answers quickly. Paging with limit(offset:) re-read everything before
            # the offset, and reading the whole series in one query (or pivoting it) is slow on long histories.
            start = f"-{self.history_window}"
            while True:
                chunk_query = f"""
                        from(bucket: "{self.bucket}")
                        |> range(start: {start})
                        |> filter(fn: (r) => r["{self.__measurement_key}"] == "{meas}")
                        {self.__entity_id_filter()}
                        {self.__prometheus_field_filter()}
                        |> limit(n: {self.chunksize})
                        """
                if self.debug:
                    print(f"\n")
                    print(f"Flux query:\n{chunk_query}\n")

                records = [record.values for record in self.__query_api.query_stream(chunk_query)]
                records, cursor = self.__split_page(records)
                if records:
                    no_lines += self.__write_batch(records)
                    self.__print_progress(meas, no_lines, field_no, len(measurements_and_fields))
                if cursor is None:
                    break
                start = cursor.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            field_no += 1

    def __split_page(self, records: List[dict]) -> tuple:
        """
        Splits one page of query results at the next cursor.

        limit() returns up to chunksize rows per table. Tables that hit the limit may have more rows, so the next
        page starts at the earliest last timestamp among them. Only rows before that cursor are kept, which means
        every timestamp kept has all of its rows (from every table), and nothing is skipped or read twice.

        :return: (rows to write, cursor for the next page or None when the series is complete)
        """
        rows_per_table: Dict[int, int] = {}
        last_time_per_table: Dict[int, datetime.datetime] = {}
        for r in records:
            rows_per_table[r["table"]] = rows_per_table.get(r["table"], 0) + 1
            last_time_per_table[r["table"]] = max(r["_time"], last_time_per_table.get(r["table"], r["_time"]))

        truncated = [last_time_per_table[t] for t, count in rows_per_table.items() if count >= self.chunksize]
        if not truncated:
            return records, None
        cursor = min(truncated)
        return [r for r in records if r["_time"] < cursor], cursor

    @staticmethod
    def __pivot_records(records: List[dict]) -> List[dict]:
        """
        Joins the field rows of each point into one row with a column per field, like Flux pivot() on _time.
        """
        rows: Dict[tuple, dict] = {}
        for r in records:
            key = tuple(sorted((k, v) for k, v in r.items()
                               if k not in ("result", "table", "_start", "_stop", "_field", "_value")))
            rows.setdefault(key, dict(key))[r["_field"]] = r["_value"]
        return list(rows.values())

    def __write_batch(self, records: List[dict]) -> int:
        """
        Converts a batch of streamed records and writes it to VictoriaMetrics (or prints it in dry-run debug mode).

        In influx mode records are converted one Flux table at a time, since tables can have different tag columns.
        In Prometheus mode each point's fields are first joined into one row.

        :return: the number of lines written
        """
        no_lines = 0
        if self.output_format == "prometheus":
            frames = [pd.DataFrame(self.__pivot_records(records))]
        else:
            # Drop columns that only belong to other tables in this batch
            frames = [df.dropna(axis=1, how="all") for _, df in pd.DataFrame(records).groupby("table", sort=False)]
        for df in frames:
            if self.output_format == "prometheus":
                lines_protocol_str = self.__get_prometheus_lines(df)
            else:
                lines_protocol_str = self.__get_influxdb_lines(df)
            if not lines_protocol_str:
                continue
            self.stats.increment(lines_protocol_str)
            no_lines += lines_protocol_str.count('\n') + 1

            if not self.dry_run:
                if self.output_format == "prometheus":
                    write_url = f"{self.vm_url}/api/v1/import/prometheus"
                else:
                    write_url = f"{self.vm_url}/write?db={self.bucket}"
                response = self.__request_with_retries("POST", write_url, data=lines_protocol_str.encode("utf8"))
                if not response.ok:
                    # Not worth retrying (e.g. VictoriaMetrics rejected the data), so report it and carry on
                    print(f"\nWrite to {write_url} failed: HTTP {response.status_code}: {response.text[:500]}")
                    logger.error(f"Write to {write_url} failed: HTTP {response.status_code}: {response.text}")
            elif self.debug:
                print(lines_protocol_str)
        return no_lines

    def __request_with_retries(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Sends a request to VictoriaMetrics, retrying connection errors, timeouts, HTTP 429 and HTTP 5xx with
        exponential back-off (1s, 2s, 4s, ... capped at 60s) up to max_retries times.

        :return: the response, which may still be an HTTP 4xx error that isn't worth retrying
        """
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.request(method, url, timeout=300, **kwargs)
                if response.status_code != 429 and response.status_code < 500:
                    return response
                error = f"HTTP {response.status_code}: {response.text[:200]}"
            except requests.RequestException as e:
                error = str(e)

            if attempt == self.max_retries:
                raise RuntimeError(f"{method} {url} failed after {self.max_retries} retries: {error}")
            delay = min(2 ** attempt, 60)
            print(f"\n{method} {url} failed ({error}), retry {attempt + 1}/{self.max_retries} in {delay}s")
            logger.warning(f"{method} {url} failed ({error}), retry {attempt + 1}/{self.max_retries} in {delay}s")
            time.sleep(delay)

    def __series_selector(self, meas: str) -> str:
        """
        PromQL series selector matching what this script writes to VictoriaMetrics for one InfluxDB series.
        """
        if self.output_format == "prometheus":
            # The entity label is "<domain>.<entity_id>", and the InfluxDB entity_id tag has no domain
            matchers = [f"entity=~{promql_string('[^.]+' + re.escape('.' + meas))}"]
            if self.job:
                matchers.append(f"job={promql_string(self.job)}")
        elif self.pivot:
            matchers = [f"entity_id={promql_string(meas)}", f"db={promql_string(self.bucket)}"]
        else:
            # VictoriaMetrics names InfluxDB data "<measurement>_<field>"
            matchers = [f"__name__=~{promql_string(re.escape(meas) + '_.+')}", f"db={promql_string(self.bucket)}"]
        return "{" + ",".join(matchers) + "}"

    def __series_exists(self, meas: str) -> bool:
        """
        Checks VictoriaMetrics' Prometheus series API for any series already written for this InfluxDB series.
        """
        selector = self.__series_selector(meas)
        if self.debug:
            print(f"Checking for existing series: {selector}")
        response = self.__request_with_retries(
            "GET", f"{self.vm_url}/api/v1/series", params={"match[]": selector, "start": "0", "limit": "1"})
        response.raise_for_status()
        return len(response.json().get("data", [])) > 0

    def __print_progress(self, meas: str, no_lines: int, field_no: int, total: int):
        print(
            f"Wrote {no_lines} total lines "
            f"from InfluxDB {self.bucket} for {meas}. "
            f"Total: {self.stats.humanized_bytes()} "
            f"({field_no}/{total})",
            end='\r')

    @staticmethod
    def __whitelist_measurements(measurements_and_fields: List) -> List[tuple]:
        """
        Applies a whitelist to the list of measurements and fields. Does nothing if no whitelist is found.

        :param measurements_and_fields :
        :return:  the new measurements and fields tuple list with the whitelist applied.
        """
        whitelist: List[tuple] = []
        whitelist_path = "whitelist.txt"
        if os.path.exists(whitelist_path):
            try:
                with open(whitelist_path, 'r') as f:
                    whitelist_rows = f.read().splitlines()

                    for row_str in whitelist_rows:
                        row = row_str.split(' ')
                        if len(row) > 3:
                            tup: tuple = row[1], row[2]
                            whitelist.append(tup)
            except OSError:
                print("Problem reading whitelist. Skipping")

            if len(whitelist) > 0:
                m_a_f_set = set(measurements_and_fields)
                whitelist_set = set(whitelist)
                measurements_and_fields = list(set.intersection(m_a_f_set, whitelist_set))

        return measurements_and_fields

    def __entity_id_filter(self) -> str:
        """
        Flux filter restricting results to the requested entity_ids, or an empty string if none were given.
        """
        if not self.entity_ids:
            return ""
        # An "or" chain of equality checks (unlike contains()) can be pushed down to InfluxDB's storage engine
        entity_ids = " or ".join(f"r.entity_id == {json.dumps(e)}" for e in self.entity_ids)
        return f'|> filter(fn: (r) => {entity_ids})'

    def __prometheus_field_filter(self) -> str:
        """
        In Prometheus mode, keeps only the fields the conversion reads. Returns an empty string otherwise.
        """
        if self.output_format != "prometheus":
            return ""
        # An "or" chain of equality checks can be pushed down to InfluxDB's storage engine. contains() can't,
        # which made Flux read every field of the series and filter it in memory.
        fields = " or ".join(f"r._field == {json.dumps(f)}" for f in PROMETHEUS_FIELDS)
        return f"|> filter(fn: (r) => {fields})"

    def __find_all_measurements(self):
        """
        Finds all permutations of measurements and fields.
        :return: a list of tuples
        """

        print("Finding unique time series:")
        first_in_series = f"""
           from(bucket: "{self.bucket}")
           |> range(start: -{self.metric_search_window})
           {self.__entity_id_filter()}
           |> filter(fn: (r) => exists r["{self.__measurement_key}"] and r["{self.__measurement_key}"] != "")
           |> keep(columns: ["{self.__measurement_key}"])
           |> group()
           |> unique(column: "{self.__measurement_key}")
        """

        if self.debug:
            print(f"Flux query:\n{first_in_series}")

        timeseries = self.__query_api.query_data_frame(first_in_series)
        if type(timeseries) is not list:
            timeseries = [timeseries]

        measurements_and_fields = set()
        for df in timeseries:
            measurements_and_fields.update(df[self.__measurement_key].unique())

        print(f"Found {len(measurements_and_fields)} unique time series:")
        if self.debug:
            for meas in sorted(measurements_and_fields):
                print(f"  {meas}")
        return measurements_and_fields

    @staticmethod
    def __get_tag_cols(dataframe_keys: Iterable) -> Iterable:
        """
        Filter out dataframe keys that are not tags

        @param dataframe_keys:
        @return:
        """
        return (
            k
            for k in dataframe_keys
            if not k.startswith("_") and k not in ["result", "table"]
        )

    def __get_influxdb_lines(self, df: pd.DataFrame) -> str:
        """
        Convert the Pandas Dataframe into InfluxDB line protocol.

        The dataframe should be similar to results received from query_api.query_data_frame()

        Not quite sure if this supports all kinds if InfluxDB schemas.
        It might be that influxdb_client package could be used as an alternative to this,
        but I'm not sure about the authorizations and such.

        Protocol description: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/
        """
        logger.info(f"Exporting {df.columns}")

        if df.empty:
            logger.debug(f"No data points for this")
            return ""

        line: str
        # Only applies to Homeassistant data migration.
        # self.__pivot guides if this is straight conversion/export or pivoting the measurements into
        # unit and having the entity ids as measurements.
        if self.pivot:
            line = df["entity_id"]
            line = df["domain"] + "." + line
        else:
            line = df["_measurement"]

        for col_name in self.__get_tag_cols(df):
            line += ("," + col_name.replace(r" ", r"\ ").replace(r",", r"\,").replace(r"=", r"\=") + "=") \
                + df[col_name].astype(str).str.replace(r" ",
                                                       r"\ ").replace(r",", r"\,").replace(r"=", r"\=")

        if self.pivot:
            line += ("," + "unit_of_measurement=") + df["_measurement"].astype(
                str).str.replace(r" ", r"\ ").replace(r",", r"\,").replace(r"=", r"\=")

        line += (
                " "
                + df["_field"].astype(str).str.replace(r" ",
                                                       r"\ ").replace(r",", r"\,").replace(r"=", r"\=")
                + "="
                + df["_value"].map(lambda x: (("\"" + (x.replace("\"", "\\\"").replace(
                    "\n", "\\n").replace("\r", "\\r")) + "\"") if (type(x) is str) else str(x)))
                + " "
                + df["_time"].astype(int).astype(str)
        )
        return "\n".join(line)

    def __prometheus_temperature(self, value) -> Optional[float]:
        """
        Climate temperature attribute in Celsius. Attributes carry no unit, so --fahrenheit says how to read them.
        """
        temperature = to_float(value)
        if temperature is not None and self.fahrenheit:
            temperature = (temperature - 32) * 5 / 9
        return temperature

    def __prometheus_samples(self, domain: str, full_entity_id: str, measurement: str, fields: Dict) \
            -> Optional[List[tuple]]:
        """
        The samples the Prometheus integration would export for one Home Assistant state change, as
        (metric name without prefix, extra labels, value) tuples. None when the domain isn't supported.
        """
        state_value = to_float(fields.get("value"))
        samples: List[tuple] = []

        if domain == "light":
            if state_value is not None:
                brightness = to_float(fields.get("brightness"))
                if state_value == 1 and brightness is not None:
                    state_value = brightness / 255.0
                samples.append(("light_brightness_percent", {}, state_value * 100))

        elif domain == "climate":
            for metric, field in [("climate_target_temperature_celsius", "temperature"),
                                  ("climate_target_temperature_high_celsius", "target_temp_high"),
                                  ("climate_target_temperature_low_celsius", "target_temp_low"),
                                  ("climate_current_temperature_celsius", "current_temperature")]:
                temperature = self.__prometheus_temperature(fields.get(field))
                if temperature is not None:
                    samples.append((metric, {}, temperature))
            samples += prometheus_enum_samples("climate_action", "action", fields.get("hvac_action_str"),
                                               PROMETHEUS_HVAC_ACTIONS)
            samples += prometheus_enum_samples("climate_mode", "mode", fields.get("state"),
                                               parse_list_attribute(fields.get("hvac_modes_str")))
            samples += prometheus_enum_samples("climate_preset_mode", "mode", fields.get("preset_mode_str"),
                                               parse_list_attribute(fields.get("preset_modes_str")))
            samples += prometheus_enum_samples("climate_fan_mode", "mode", fields.get("fan_mode_str"),
                                               parse_list_attribute(fields.get("fan_modes_str")))

        elif domain == "cover":
            cover_state = fields.get("state")
            if is_missing(cover_state) and state_value is not None:
                cover_state = "open" if state_value else "closed"
            samples += prometheus_enum_samples("cover_state", "state", cover_state, PROMETHEUS_COVER_STATES)
            for metric, field in [("cover_position", "current_position"),
                                  ("cover_tilt_position", "current_tilt_position")]:
                position = to_float(fields.get(field))
                if position is not None:
                    samples.append((metric, {}, position))

        elif domain in ("sensor", "counter") or domain in PROMETHEUS_NUMERIC_STATE_DOMAINS:
            if state_value is None:
                return samples
            # Home Assistant uses the unit as the measurement, or the full entity_id when there is no unit
            raw_unit = None if measurement == full_entity_id else measurement
            unit = prometheus_unit_string(raw_unit)
            if raw_unit == "°F":
                state_value = (state_value - 32) * 5 / 9

            if domain == "sensor":
                device_class = fields.get("device_class_str")
                if device_class == "timestamp":
                    return samples
                if unit and not is_missing(device_class):
                    metric = f"sensor_{device_class}_{unit}"
                elif unit:
                    metric = f"sensor_unit_{unit}"
                else:
                    metric = "sensor_state"
            elif domain == "counter":
                metric = "counter_value"
            else:
                metric = f"{domain}_state_{unit}" if unit else f"{domain}_state"
            samples.append((metric, {}, state_value))

        else:
            return None
        return samples

    def __get_prometheus_lines(self, df: pd.DataFrame) -> str:
        """
        Convert the pivoted Pandas Dataframe (one row per Home Assistant state change, one column per field) into
        the Prometheus exposition format, named and labelled the way the Home Assistant Prometheus integration
        does it, with millisecond timestamps.

        Rows from domains the conversion doesn't support are skipped.

        Format description: https://docs.victoriametrics.com/#how-to-import-data-in-prometheus-exposition-format
        """
        logger.info(f"Exporting {df.columns}")

        lines = []
        skipped_domains = set()
        for row in df.to_dict("records"):
            fields = {k: v for k, v in row.items() if not is_missing(v)}
            domain = fields["domain"]
            full_entity_id = f"{domain}.{fields['entity_id']}"
            if fields.get("state") in ("unavailable", "unknown"):
                continue

            samples = self.__prometheus_samples(domain, full_entity_id, fields["_measurement"], fields)
            if samples is None:
                skipped_domains.add(domain)
                continue

            friendly_name = fields.get("friendly_name", fields.get("friendly_name_str", ""))
            base_labels = {"domain": domain, "entity": full_entity_id, "friendly_name": str(friendly_name)}
            if self.job:
                base_labels["job"] = self.job
            timestamp_ms = pd.Timestamp(fields["_time"]).value // 1_000_000

            for metric, extra_labels, value in samples:
                metric_name = prometheus_sanitize_metric_name(f"{self.prometheus_prefix}{metric}")
                labels = ",".join(f'{k}="{prometheus_escape_label(v)}"'
                                  for k, v in sorted({**base_labels, **extra_labels}.items()))
                lines.append(f"{metric_name}{{{labels}}} {value} {timestamp_ms}")

        if skipped_domains:
            logger.debug(f"Skipped domains without Prometheus support: {skipped_domains}")
        return "\n".join(lines)


def main(args: Dict[str, str]):
    logger.info("args: " + str(args.keys()))
    bucket = args.pop("bucket")
    vm_url = args.pop("VM_ADDR")
    if vm_url is None:
        vm_url = os.environ['VM_ADDR']
    dry_run = bool(args.pop("dry_run"))
    pivot = bool(args.pop("pivot"))
    debug = bool(args.pop("debug"))
    output_format = args.pop("output_format")
    prometheus_namespace = args.pop("prometheus_namespace")
    fahrenheit = bool(args.pop("fahrenheit"))
    job = args.pop("job")
    max_retries = args.pop("max_retries")
    skip_existing = bool(args.pop("skip_existing"))
    entity_ids = [e.strip() for arg in (args.pop("entity_id") or []) for e in arg.split(",") if e.strip()]
    history_window = args.pop("history_window") or "100d"
    metric_search_window = args.pop("metric_search_window") or history_window
    chunk_size = args.pop("chunk_size")

    if entity_ids:
        print(f"Filtering to {len(entity_ids)} entity_id(s): {', '.join(entity_ids)}")

    print(f"Dry run: {dry_run}, Pivot: {pivot}, Debug: {debug}, Output format: {output_format}, History window: {history_window}, "
          f"Metric search window: {metric_search_window}, Chunk size: {chunk_size}")

    for k, v in args.items():
        if v is not None:
            os.environ[k] = v
        logger.info(f"Using {k}={os.getenv(k)}")

    migrator = InfluxMigrator(bucket, vm_url, chunksize=chunk_size, dry_run=dry_run, pivot=pivot,
                              history_window=history_window, metric_search_window=metric_search_window,
                              debug=debug, entity_ids=entity_ids, output_format=output_format,
                              prometheus_namespace=prometheus_namespace, fahrenheit=fahrenheit,
                              job=job, max_retries=max_retries, skip_existing=skip_existing)
    migrator.influx_connect()
    migrator.migrate()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Script for exporting InfluxDB data into victoria metrics instance. \n"
                    " InfluxDB settings can be defined on command line or as environment variables"
                    " (or in .env file if python-dotenv is installed)."
                    " InfluxDB related args described in \n"
                    "https://github.com/influxdata/influxdb-client-python#via-environment-properties"
    )
    parser.add_argument(
        "bucket",
        type=str,
        help="InfluxDB source bucket",
    )
    parser.add_argument(
        "--INFLUXDB_V2_ORG",
        "-o",
        type=str,
        help="InfluxDB organization",
    )
    parser.add_argument(
        "--INFLUXDB_V2_URL",
        "-u",
        type=str,
        help="InfluxDB Server URL, e.g., http://localhost:8086",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TOKEN",
        "-t",
        type=str,
        help="InfluxDB access token.",
    )
    parser.add_argument(
        "--INFLUXDB_V2_SSL_CA_CERT",
        "-S",
        type=str,
        help="Server SSL Cert",
    )
    parser.add_argument(
        "--INFLUXDB_V2_TIMEOUT",
        "-T",
        type=str,
        help="InfluxDB timeout",
    )
    parser.add_argument(
        "--INFLUXDB_V2_VERIFY_SSL",
        "-V",
        type=str,
        help="Verify SSL CERT",
    )
    parser.add_argument(
        "--VM_ADDR",
        "-a",
        type=str,
        help="VictoriaMetrics server URL, e.g., http://localhost:8428",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action='store_true',
        default=False,
        help="Dry run",
    )
    parser.add_argument(
        "--pivot",
        "-P",
        action='store_true',
        default=False,
        help="Pivot entity_id to be measurement",
    )
    parser.add_argument(
        "--output-format",
        "-f",
        choices=["influx", "prometheus"],
        default="influx",
        help="Format written to VictoriaMetrics. 'influx' copies the InfluxDB series as-is (line protocol). "
             "'prometheus' converts Home Assistant data to the metric names and labels the Home Assistant "
             "Prometheus integration uses, e.g. homeassistant_sensor_battery_percent{domain=...,entity=...,"
             "friendly_name=...}. Default: influx",
    )
    parser.add_argument(
        "--prometheus-namespace",
        type=str,
        default="homeassistant",
        help="Metric name prefix for --output-format prometheus, matching the Prometheus integration's "
             "'namespace' setting. Pass an empty string for no prefix. Default: homeassistant",
    )
    parser.add_argument(
        "--fahrenheit",
        action='store_true',
        default=False,
        help="For --output-format prometheus: Home Assistant uses Fahrenheit, so climate temperature attributes "
             "are converted to Celsius like the Prometheus integration does",
    )
    parser.add_argument(
        "--job",
        type=str,
        default=None,
        help="For --output-format prometheus: add a job label with this value to every metric, matching the "
             "job name of the scrape config that collects Home Assistant's Prometheus metrics",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=99,
        help="How many times to retry a failed request to VictoriaMetrics (connection errors, timeouts, HTTP 429 "
             "and 5xx), with exponential back-off capped at 60s between attempts. Default: 99",
    )
    parser.add_argument(
        "--skip-existing",
        action='store_true',
        default=False,
        help="Skip any series that already has data in VictoriaMetrics, checked with its Prometheus series API",
    )
    parser.add_argument(
        "--entity-id",
        "-e",
        action='append',
        help="Only migrate series with this entity_id. Repeat the flag or pass a comma-separated list "
             "(e.g. -e sensor_a,sensor_b -e sensor_c)",
    )
    parser.add_argument(
        "--debug",
        "-d",
        action='store_true',
        default=False,
        help="Print extra diagnostic output, such as the list of discovered time series",
    )
    parser.add_argument(
        "--history-window",
        type=str,
        default="100d",
        help="How far back to migrate, as an InfluxDB Flux duration (e.g. 100d, 730d, 2y). Default: 100d",
    )
    parser.add_argument(
        "--metric-search-window",
        type=str,
        default=None,
        help="How far back to look when discovering which series to migrate, as an InfluxDB Flux duration "
             "(e.g. 1d, 30d). Default: same as --history-window",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Maximum rows read per InfluxDB field per query, and so roughly per write to VictoriaMetrics. "
             "Larger chunks mean fewer queries but more memory. Default: 100000",
    )

    parsed_args = parser.parse_args()
    # Paging needs at least two rows per chunk to make progress
    if parsed_args.chunk_size < 2:
        parser.error("--chunk-size must be at least 2")
    if parsed_args.max_retries < 0:
        parser.error("--max-retries can't be negative")
    main(vars(parsed_args))
    print("All done")
