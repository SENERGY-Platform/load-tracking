# Load Tracking

## Run detection on recorded data

`algo/run_tracker.py` is a candidate replacement for the run detection in `algo/load_device.py`.
It is not wired into the operator yet. It decides whether a near-zero phase is a pause inside a
run (drying, eco soak) or the end of the run only after the phase is over. The idle level is
learned per device instead of using fixed 10 W / 1.5 W thresholds.

`scripts/replay_loads.py` plays a recorded device through both detections and compares them:

    python scripts/replay_loads.py replay export.csv --labels labels.csv -o runs.csv

The export holds one device with `time` (ISO 8601, UTC without offset), `power` (W) and,
optionally, `energy` (counter in kWh, `--energy-unit Wh` otherwise). `labels.csv` holds `start`
and `end` of known runs (Europe/Berlin without offset).

`fetch` caches the history of platform devices as such exports. It reads through
timescale-wrapper with a platform token in `SENERGY_TOKEN` and needs operator-lib 1.14 with
pandas 2, which this repository does not pin yet, so it runs in its own environment:

    uv run --no-project --python 3.10 --with "operator-lib @ git+https://github.com/SENERGY-Platform/analytics-operator-lib-python@v1.14.0" \
        python scripts/replay_loads.py fetch devices.json cache/ --days 60

The tests run with `python -m unittest discover -s tests`. Measurements and design decisions are
in [docs/](docs/), the comparison on recorded appliances in
[docs/run-detection-measured-on-recorded-appliances.md](docs/run-detection-measured-on-recorded-appliances.md).

## Config options
<!---
| key                | type                                                 | description                                               | required |
|--------------------|------------------------------------------------------|-----------------------------------------------------------|----------|
|     |                                              |                      |     |
| `weather_src_id`   | string                                               | ID of source providing weather forecast data.             | yes      |
| `logger_level`     | string                                               | `info`, `warning` (default), `error`, `critical`, `debug` | no       |
| `selectors`        | array[object{"name": string, "args": array[string]}] | Define selectors to distinguish between data sources.     | no       |
| `power_td`         | float                                                | Time difference between consecutive power values in min (default:0.17)   | no       |
| `weather_dim`      | integer                                              |                                                           | no       |
| `data_path`        | string                                               | Path to reward and model files. Default: "/opt/data"      | no       |
| `buffer_len`       | integer                                              | Length of replay buffer (default: 48)                     | no       |
| `p_1`              | integer                                              | Power for reward calculation if action==1  (default:1)    | no       |
| `p_0`              | integer                                              | Power for reward calculation if action==0  (default:1)    | no       |
| `history_modus`    | string                                               | `all`, `daylight` (default)                               | no       |

-->
