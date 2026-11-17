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
Synthetic smart-plug streams with known runs: instantaneous power in W (with noise) and an exact
energy counter in kWh. The profiles are modelled on the phases of real programs, but they are
not recordings; they show that the mechanics work, not how accurate detection is on real devices.
"""

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

T0 = datetime(2026, 3, 2, tzinfo=timezone.utc)


def pump_phase(rng, total_s, p_on, p_off):
    """Circulation pump with short stops of 30-90 s, as dish washers do."""
    segs, t = [], 0.0
    while t < total_s:
        on = min(rng.uniform(120, 240), total_s - t)
        segs.append((on, p_on + rng.uniform(-3, 3)))
        t += on
        if t < total_s:
            off = rng.uniform(30, 90)
            segs.append((off, p_off))
            t += off
    return segs


def dishwasher(rng, mid_pause=False, pause_w=0.8, heat=1.0):
    s = pump_phase(rng, 10 * 60, 35, pause_w)                  # pre-wash
    s += [(120, 30), (15 * 60 * heat, 2050)]                  # drain, heat main wash
    s += pump_phase(rng, 25 * 60, 40, pause_w)
    if mid_pause:
        s += [(rng.uniform(22, 30) * 60, pause_w)]            # eco soak, everything off
        s += pump_phase(rng, 10 * 60, 40, pause_w)
    s += [(120, 30)] + pump_phase(rng, 8 * 60, 40, pause_w)   # intermediate rinse
    s += [(120, 30), (10 * 60 * heat, 2050)]                  # final rinse heat
    s += pump_phase(rng, 8 * 60, 40, pause_w) + [(120, 30)]
    s += [(rng.uniform(35, 60) * 60, pause_w)]                # passive drying at ~0 W
    s += [(rng.uniform(40, 90), 25)]                          # final pump-out / AutoOpen
    return s


def drum(rng, total_s, p_on=180, p_off=3):
    segs, t = [], 0.0
    while t < total_s:
        segs += [(12, p_on + rng.uniform(-20, 20)), (4, p_off)]
        t += 16
    return segs


def washer(rng):
    """Programs from 30 to 60 degC: 0 to 25 min heating, one to three rinses."""
    heat = rng.choice([0, 0, 8, 15, 25]) * 60
    s = [(120, 8)] + drum(rng, 5 * 60) + ([(heat, 2100)] if heat else [])
    s += drum(rng, rng.uniform(20, 45) * 60) + [(90, 40)]
    for _ in range(rng.choice([1, 2, 3])):
        s += [(120, 8)] + drum(rng, 8 * 60) + [(90, 40), (5 * 60, 350)]
    s += [(rng.uniform(4, 10) * 60, 450)]
    return s


@dataclass
class TruthRun:
    start: datetime
    end: datetime
    energy_kwh: float


def stream(seed, days, make_runs, standby_w=0.5, standby_sd=0.05, dt_s=60):
    """
    One run per day between 08:00 and 20:00 UTC; make_runs(rng) returns [(gap_before_s, segments)],
    more than one entry for loads started back to back. Returns ([(ts, power, counter)], [TruthRun]).
    """
    rng = random.Random(seed)
    segs, truth, t = [], [], 0.0
    for d in range(days):
        start = d * 86400.0 + rng.uniform(8, 20) * 3600
        if start > t:
            segs.append((t, start - t, standby_w))
            t = start
        for gap_s, program in make_runs(rng):
            if gap_s:
                segs.append((t, gap_s, standby_w))
                t += gap_s
            run_start, e = t, 0.0
            for dur, p in program:
                segs.append((t, dur, p))
                e += dur * p
                t += dur
            truth.append(TruthRun(T0 + timedelta(seconds=run_start), T0 + timedelta(seconds=t), e / 3.6e6))
    end = days * 86400.0
    segs.append((t, end - t, standby_w))

    samples, i, cum, ts = [], 0, 0.0, 0.0
    while ts < end:
        while segs[i][0] + segs[i][1] <= ts:
            cum += segs[i][1] * segs[i][2]
            i += 1
        seg_t, _, p = segs[i]
        noise_sd = standby_sd if p == standby_w else 0.02 * p + 0.05
        power = max(0.0, p + rng.gauss(0, noise_sd))
        samples.append((T0 + timedelta(seconds=ts), round(power, 2), (cum + (ts - seg_t) * p) / 3.6e6))
        ts += dt_s
    return samples, truth


def match(truth, detected):
    """detected: [(start, end)]. Returns (splits, merges, missed, max start error, max end error)."""
    splits = merges = missed = 0
    start_err = end_err = timedelta()
    for t in truth:
        hits = [d for d in detected if d[0] < t.end and d[1] > t.start]
        if not hits:
            missed += 1
            continue
        splits += len(hits) > 1
        start_err = max(start_err, abs(hits[0][0] - t.start))
        end_err = max(end_err, abs(hits[-1][1] - t.end))
    for d in detected:
        merges += sum(1 for t in truth if d[0] < t.end and d[1] > t.start) > 1
    return splits, merges, missed, start_err, end_err
