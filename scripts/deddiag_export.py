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

"""Convert one DEDDIAG item (lazy 1 Hz, UTC) into the export format of scripts/replay_loads.py.

DEDDIAG is a public dataset of appliances in German homes, doi:10.6084/m9.figshare.13615073
(CC BY 4.0), one zip per house. Its readings are stored only when the value changes, plus one at
every full hour, and their timestamps are UTC: there are readings in the hour the clocks skip in
spring. docs/run-detection-measured-on-recorded-appliances.md describes the measurement.

    python scripts/deddiag_export.py <house_dir> <item_id> <dt_s> <out_dir>

Writes <out_dir>/h<house>_i<item>_<dt>s.parquet with time (UTC), power (W, instantaneous value at
each sample, as a smart plug reports it) and energy (kWh, exact integral of the 1 Hz step function),
and, when the item has annotations, <out_dir>/h<house>_i<item>_labels.csv (start, end, label; UTC).
A stretch without any record for more than 65 min is a logger gap: no samples are emitted there
and the counter stays flat.
"""
import os
import sys

import numpy as np
import pandas as pd

GAP = pd.Timedelta(minutes=65)


def main(house_dir, item, dt_s, out_dir):
    item = int(item)
    dt = pd.Timedelta(seconds=int(dt_s))
    house = os.path.basename(os.path.normpath(house_dir)).split("_")[1]
    raw = pd.read_csv(os.path.join(house_dir, f"item_{item:04d}_data.tsv.gz"), sep="\t", usecols=["time", "value"])
    raw["time"] = pd.to_datetime(raw["time"])
    raw = raw.dropna().sort_values("time", kind="stable").drop_duplicates("time", keep="last").reset_index(drop=True)

    t = raw["time"].values
    v = raw["value"].to_numpy(dtype=float)
    hold = np.diff(t).astype("timedelta64[ns]").astype(np.int64) / 1e9          # seconds each value holds
    gap = hold > GAP.total_seconds()
    seg_energy = np.where(gap, 0.0, v[:-1] * hold) / 3.6e6                        # kWh per segment
    cum = np.concatenate([[0.0], np.cumsum(seg_energy)])                          # counter at each record

    grid = pd.date_range(raw["time"].iloc[0].ceil(dt), raw["time"].iloc[-1], freq=dt).values
    idx = np.searchsorted(t, grid, side="right") - 1                              # last record at or before
    since = (grid - t[idx]).astype("timedelta64[ns]").astype(np.int64) / 1e9
    in_gap = np.zeros(len(grid), dtype=bool)
    valid = idx < len(gap)
    in_gap[valid] = gap[idx[valid]]
    power = v[idx]
    energy = cum[idx] + np.where(in_gap, 0.0, power * since / 3.6e6)

    out = pd.DataFrame({"time": pd.to_datetime(grid).tz_localize("UTC"), "power": power, "energy": energy})[~in_gap]
    os.makedirs(out_dir, exist_ok=True)
    name = f"h{house}_i{item}"
    out.to_parquet(os.path.join(out_dir, f"{name}_{int(dt_s)}s.parquet"), index=False)
    print(f"{name}: {len(raw)} records -> {len(out)} samples every {dt_s} s, "
          f"{out['time'].iloc[0]} .. {out['time'].iloc[-1]}, {gap.sum()} gaps, {out['energy'].iloc[-1]:.1f} kWh")

    ann_path = os.path.join(house_dir, f"item_{item:04d}_annotations.tsv")
    ann = pd.read_csv(ann_path, sep="\t")
    if len(ann):
        labels = pd.read_csv(os.path.join(house_dir, f"item_{item:04d}_annotation_labels.tsv"), sep="\t")
        ann = ann.merge(labels[["id", "text"]], left_on="label_id", right_on="id", how="left")
        lab = pd.DataFrame({"start": pd.to_datetime(ann["start_date"]).dt.tz_localize("UTC"),
                            "end": pd.to_datetime(ann["stop_date"]).dt.tz_localize("UTC"),
                            "label": ann["text"]}).sort_values("start")
        lab.to_csv(os.path.join(out_dir, f"{name}_labels.csv"), index=False)
        print(f"{name}: {len(lab)} annotations {lab['label'].value_counts().to_dict()}, "
              f"{lab['start'].min()} .. {lab['end'].max()}")


if __name__ == "__main__":
    main(*sys.argv[1:])
