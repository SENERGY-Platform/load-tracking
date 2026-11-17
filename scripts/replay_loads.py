"""
   Copyright 2026 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

"""
Replays one recorded device through both load detections (calibration aid, not a test):

    current      algo.load_device as the operator runs it today, unchanged and fed exactly
                 like main.Operator.run feeds it
    run_tracker  the candidate algo.run_tracker

    fetch   read --days of power and energy per device through timescale-wrapper and cache them
            as parquet, one file per device; needs a platform token in SENERGY_TOKEN, operator-lib
            1.14 and pandas 2, which this repository does not pin yet, hence the uv line below
    replay  play one export through both detections; prints a summary per variant and, with
            --labels, how well each found the known runs; -o writes every run of both to a CSV

The devices file is a JSON list of {"name", "device_id", "service_id", "power", "energy" (optional),
"end" (optional ISO time)}; "power" and "energy" are paths in the service output, e.g.
"energy.power". The export is a CSV or parquet file of ONE device with the columns
    time    ISO 8601; read as UTC when it carries no offset
    power   W
    energy  energy counter in kWh, or Wh with --energy-unit Wh (optional; integrated from power)
The labels file is a CSV with `start` and `end` of known runs; read as Europe/Berlin when the
times carry no offset, since that is how people note them.

    uv run --no-project --python 3.10 --with "operator-lib @ git+https://github.com/SENERGY-Platform/analytics-operator-lib-python@v1.14.0" \\
        python scripts/replay_loads.py fetch devices.json cache/ --days 60
    python scripts/replay_loads.py replay cache/dishwasher.parquet --labels labels.csv -o runs.csv

The current algorithm re-reads its whole buffer on every low sample while a run is active, so a
long export takes minutes; --skip-current leaves it out.
"""

import argparse
import contextlib
import datetime
import io
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from algo import load_device
from algo.run_tracker import Params, RunTracker

LOCAL = "Europe/Berlin"
WRAPPER_URL = "https://api.senergy.infai.org/db/v3"


def fetch(devices, cache, days):
    from operator_lib.util.helpers.ts_wrapper import read_history
    from operator_lib.util.model import InputTopic

    os.makedirs(cache, exist_ok=True)
    token = os.environ["SENERGY_TOKEN"]
    for device in devices:
        end = pd.Timestamp(device["end"]) if device.get("end") else pd.Timestamp.now(tz="UTC")
        end = (end.tz_localize("UTC") if end.tzinfo is None else end).to_pydatetime()
        mappings = [{"dest": "power", "source": "value." + device["power"]}]
        if device.get("energy"):
            mappings.append({"dest": "energy", "source": "value." + device["energy"]})
        topic = InputTopic({"name": device["service_id"].replace(":", "_"), "filterType": "DeviceId",
                            "filterValue": device["device_id"], "mappings": mappings})
        path = os.path.join(cache, device["name"].replace(" ", "_") + ".parquet")
        for attempt in range(3):
            try:
                frame = read_history(WRAPPER_URL, token, topic, datetime.timedelta(days=days), False, end)
                frame.to_parquet(path, index=False)
                print(device["name"], len(frame), "rows ->", path, flush=True)
                break
            except Exception as ex:
                print(device["name"], "attempt", attempt + 1, type(ex).__name__, str(ex)[:200], flush=True)


def read_export(path):
    """Returns the frame and whether the energy had to be integrated from power (then in kWh)."""
    frame = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
    missing = {"time", "power"} - set(frame.columns)
    if missing:
        sys.exit(f"{path}: missing columns {sorted(missing)}")
    frame["time"] = pd.to_datetime(frame["time"], utc=True)
    frame = frame.sort_values("time", kind="stable").drop_duplicates("time", keep="last")
    integrated = "energy" not in frame or frame["energy"].isna().all()
    if integrated:
        seconds = frame["time"].diff().dt.total_seconds().fillna(0)
        frame["energy"] = (frame["power"].ffill().fillna(0).shift(fill_value=0) * seconds).cumsum() / 3.6e6
        print("no energy column: integrated from power, in kWh", file=sys.stderr)
    frame["energy"] = frame["energy"].ffill()
    return frame.dropna(subset=["power", "energy"]).reset_index(drop=True), integrated


def _local(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return ts.tz_localize(LOCAL, ambiguous="NaT", nonexistent="shift_forward")
    return ts.tz_convert(LOCAL)


def _progress(i, n, name):
    if n >= 10 and i % (n // 10) == 0:
        print(f"  {name}: {100 * i // n} %", file=sys.stderr, flush=True)


def run_current(frame):
    """The loop of main.Operator.run for selector "device_data", including its timestamp handling."""
    power_list, energy_list, loads, active = [], [], [], False
    mean_features = {'mean_consumed_energy': 0, 'mean_max_power': 0, 'mean_length': 0, 'mean_threshold': 0}
    n = len(frame)
    with contextlib.redirect_stdout(io.StringIO()):
        for i, (t, p, e) in enumerate(zip(frame["time"], frame["power"], frame["energy"])):
            _progress(i, n, "current")
            timestamp = pd.Timestamp(t).tz_convert(LOCAL).tz_localize(None)
            energy_list.append([timestamp, float(e)])
            power_list.append([timestamp, float(p)])
            old = len(loads)
            power_list, energy_list, loads, mean_features, active = load_device.online_tracking_loads(
                power_list, energy_list, loads, mean_features, active)
            if len(loads) > old:
                energy_list, power_list = [], []
    rows = [dict(variant="current", start=_local(load.power_series.index[0]), end=_local(load.power_series.index[-1]),
                 duration_min=load.length.total_seconds() / 60, energy=load.consumed_energy,
                 peak_w=load.max_power, q80_w=load.threshold)
            for load in loads]
    note = (f"operator's mean_features: length {mean_features['mean_length'] / 60:.1f} min, "
            f"energy {mean_features['mean_consumed_energy']:.4g}, threshold {mean_features['mean_threshold']:.1f} W; "
            f"still active at the end: {active}, buffer {len(power_list)} samples")
    return rows, note


def run_new(frame, counter_wh=1000.0):
    tracker = RunTracker(Params(counter_wh=counter_wh))
    n = len(frame)
    for i, (t, p, e) in enumerate(zip(frame["time"], frame["power"], frame["energy"])):
        _progress(i, n, "run_tracker")
        tracker.update(t.to_pydatetime(), float(p), float(e))
    tracker.flush()
    rows = [dict(variant="run_tracker", start=_local(r.start), end=_local(r.end),
                 duration_min=r.length.total_seconds() / 60, energy=r.energy, peak_w=r.peak_w,
                 active_min=r.active.total_seconds() / 60, pause_min=r.pause.total_seconds() / 60,
                 core_min=r.core.total_seconds() / 60, fragments=r.n_fragments, minor=r.minor)
            for r in tracker.runs]
    learned = f"{tracker.learned_gap.total_seconds() / 60:.0f} min" if tracker.learned_gap else "none"
    note = (f"idle level at the end {tracker.baseline.level():.2f} W, learned pause {learned}, "
            f"revisions of final runs {tracker.revisions}, minor runs {sum(r.minor for r in tracker.runs)}")
    return rows, note


def read_labels(path):
    labels = pd.read_csv(path)
    return [(_local(s), _local(e)) for s, e in zip(labels["start"], labels["end"])]


def compare(labels, runs):
    """
    Splits, merges, missed and the start and end errors over the labelled runs. Minor runs count:
    a separate pre-rinse is a run of its own, it is only left out of the run statistics.
    """
    detected = [(r["start"], r["end"]) for r in runs]
    splits = merges = missed = 0
    start_err, end_err = [], []
    for start, end in labels:
        hits = [d for d in detected if d[0] < end and d[1] > start]
        if not hits:
            missed += 1
            continue
        splits += len(hits) > 1
        start_err.append(abs((hits[0][0] - start).total_seconds()) / 60)
        end_err.append(abs((hits[-1][1] - end).total_seconds()) / 60)
    for d in detected:
        merges += sum(1 for s, e in labels if d[0] < e and d[1] > s) > 1
    span = (min(s for s, _ in labels), max(e for _, e in labels))
    unlabelled = sum(1 for d in detected if span[0] <= d[0] <= span[1]
                     and not any(d[0] < e and d[1] > s for s, e in labels))

    def err(v):
        # the share beyond 5 min, because a median hides a minority of runs that are far off
        return (f"median {statistics.median(v):.1f} / max {max(v):.1f} min, "
                f"{sum(x > 5 for x in v)} of {len(v)} over 5 min") if v else "-"

    return (f"labels {len(labels)}: split {splits}, merged {merges}, missed {missed}, "
            f"unlabelled runs within the labelled span {unlabelled}; start error {err(start_err)}, "
            f"end error {err(end_err)}")


def summary(rows):
    major = [r for r in rows if not r.get("minor")]
    if not major:
        return "0 runs"
    return (f"{len(major)} runs, duration median {statistics.median(r['duration_min'] for r in major):.1f} min, "
            f"energy median {statistics.median(r['energy'] for r in major):.4g}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    fetch_cmd = commands.add_parser("fetch", help="cache device history from the platform as parquet")
    fetch_cmd.add_argument("devices", help="JSON list of devices")
    fetch_cmd.add_argument("cache", help="directory for one parquet file per device")
    fetch_cmd.add_argument("--days", type=int, default=60)
    replay_cmd = commands.add_parser("replay", help="play one export through both detections")
    replay_cmd.add_argument("export", help="CSV or parquet with time, power[, energy] of one device")
    replay_cmd.add_argument("--labels", help="CSV with start, end of known runs")
    replay_cmd.add_argument("-o", "--output", help="write every run of both variants to this CSV")
    replay_cmd.add_argument("--energy-unit", choices=("kWh", "Wh"), default="kWh", help="unit of the counter")
    replay_cmd.add_argument("--skip-current", action="store_true", help="leave out today's algorithm")
    args = parser.parse_args()

    if args.command == "fetch":
        with open(args.devices, encoding="utf-8") as f:
            fetch(json.load(f), args.cache, args.days)
        return

    frame, integrated = read_export(args.export)
    step = frame["time"].diff().median()
    print(f"{args.export}: {len(frame)} samples, {frame['time'].iloc[0]} .. {frame['time'].iloc[-1]}, "
          f"median step {step.total_seconds():.0f} s")
    labels = read_labels(args.labels) if args.labels else None
    counter_wh = 1000.0 if integrated or args.energy_unit == "kWh" else 1.0

    variants = [] if args.skip_current else [("current", run_current)]
    variants.append(("run_tracker", lambda f: run_new(f, counter_wh)))
    all_rows = []
    for name, run in variants:
        began = time.perf_counter()
        rows, note = run(frame)
        print(f"\n{name} ({time.perf_counter() - began:.1f} s): {summary(rows)}\n  {note}")
        if labels:
            print("  " + compare(labels, rows))
        all_rows += rows

    if args.output:
        pd.DataFrame(all_rows).to_csv(args.output, index=False)
        print(f"\n{len(all_rows)} runs written to {args.output}")


if __name__ == "__main__":
    main()
