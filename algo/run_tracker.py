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
Unsupervised start and finish detection for appliances that run in programs (dish washers,
washing machines, dryers). Candidate replacement for load_device.online_tracking_loads; not
wired into the operator yet, scripts/replay_loads.py compares both on recorded data.

Whether a stretch of near-zero power is a pause inside a run (drying, eco soak) or the end of
the run cannot be decided while it lasts, only afterwards. So this decides afterwards:

  1. Activity:  power above an adaptive idle level plus a margin, instead of fixed 10 W / 1.5 W.
                The idle level follows 0.3 W standby as well as 1.8 W network standby.
  2. Fragments: stretches of activity; inactivity shorter than `min_gap` stays inside a fragment.
                A fragment that opens with a long wait at a few watts, such as the countdown of
                a delayed start, begins where the work begins (see _skip_waiting).
  3. Runs:      fragments grouped across longer pauses, decided once the fragment after the
                pause is complete:
                  - tail rule: a short, small fragment after a pause (pump-out, AutoOpen door)
                    belongs to the run before it
                  - learned pause: a gap length the device shows regularly, learned from its
                    own history without labels (see learn_gap)
                A run is finalized once the device has been idle longer than any pause that
                could still be bridged. Its end is the last active sample, not that moment.

Runs are derived from the stored fragments. When the learned pause changes, all runs are
derived again, so runs split before enough history existed are repaired retroactively.

Timestamps must increase and should be UTC; local time repeats an hour when the clocks go back.
Power in W; the energy counter in kWh (Params.counter_wh for another unit). The counter decides
whether the device was active while no sample arrived: a plug that was offline for two days must
not stretch the fragment it left open across those days.
"""

__all__ = ("Params", "Run", "RunTracker")

import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from statistics import median


@dataclass
class Params:
    min_gap: timedelta = timedelta(minutes=5)       # always bridged (stays one fragment)
    tail_max_gap: timedelta = timedelta(hours=2)    # tail rule applies up to this gap
    pause_cap: timedelta = timedelta(hours=2)       # longest pause the learner may accept
    tail_ratio: float = 0.05                        # tail if E(fragment) < ratio * E(run so far) ...
    tail_max_dur: timedelta = timedelta(minutes=15) # ... and the fragment is short
    abs_margin_w: float = 3.0                       # activity: power > idle + max(abs, rel * idle)
    rel_margin: float = 0.5
    max_idle_w: float = 15.0                        # an idle level above this is implausible
    wait_min: timedelta = timedelta(minutes=10)     # this long below max_idle_w opening a fragment
                                                    # is waiting (delayed start), not work, ...
    wait_energy_share: float = 0.05                 # ... while less than this share of its energy
                                                    # lies before the wait
    counter_wh: float = 1000.0                      # Wh per counter unit: 1000 for kWh, 1 for Wh
    min_runs_to_learn: int = 5
    min_score_gain: float = 0.02                    # decades, IQR(log E) + IQR(log duration)
    max_pause_spread: float = 2.0                   # learned pauses: longest/shortest bridged gap
    min_recurrence: float = 0.25                    # learned pause must occur in >= 25 % of runs
    minor_ratio: float = 0.05                       # runs below 5 % of median energy are "minor"


class IdleBaseline:
    """
    Idle level = 20th percentile of the per-minute minima over the last 24 h. Appliances are idle
    most of the day, so this follows whatever standby the device has without configuration.
    """

    def __init__(self, window=timedelta(hours=24), q=0.2, max_idle_w=15.0):
        self.window, self.q, self.max_idle_w = window, q, max_idle_w
        self.minutes = deque()
        self._level = None

    def add(self, ts: datetime, p: float):
        key = ts.replace(second=0, microsecond=0)
        if self.minutes and self.minutes[-1][0] == key:
            self.minutes[-1][1] = min(self.minutes[-1][1], p)
            if self._level is None:
                self._recompute()
            return
        self.minutes.append([key, p])
        while self.minutes[0][0] < key - self.window:
            self.minutes.popleft()
        self._recompute()

    def _recompute(self):
        vals = sorted(m for _, m in self.minutes)
        self._level = min(vals[int(self.q * (len(vals) - 1))], self.max_idle_w)

    def level(self) -> float:
        return self._level if self._level is not None else 0.0


@dataclass
class Fragment:
    start: datetime          # last idle sample before the activity: the energy reference point
    end: datetime            # last active sample
    e_start: float
    e_end: float
    peak: float
    samples: list = field(default_factory=list)   # (ts, counter, power); power None for the reference

    @property
    def energy(self):
        return self.e_end - self.e_start


def _skip_waiting(f, p):
    """
    No program begins with ten minutes below any plausible idle level. A fragment that does began
    with a waiting state, a delayed start for instance, and starts where the work starts. A blip
    before the wait (the button, the door) does not change that; so the wait is cut off as long as
    less than wait_energy_share of the fragment's energy lies before it. A fragment that never
    rises above that level is kept whole: it may be the drying fan at the end of a run.
    """
    limit = f.e_start + p.wait_energy_share * f.energy
    cut, low_since = None, None
    for i in range(1, len(f.samples)):
        ts, e, power = f.samples[i]
        if power < p.max_idle_w:
            if low_since is None:
                low_since = f.samples[i - 1][0]
            continue
        prev_ts, prev_e, _ = f.samples[i - 1]
        if low_since is not None and prev_ts - low_since >= p.wait_min and prev_e <= limit:
            cut = i - 1
        low_since = None
        if e > limit:
            break
    if cut is None:
        return f
    ref_ts, ref_e, _ = f.samples[cut]
    samples = [(ref_ts, ref_e, None)] + f.samples[cut + 1:]
    return Fragment(ref_ts, f.end, ref_e, f.e_end, max(s[2] for s in samples[1:]), samples)


@dataclass
class Run:
    start: datetime
    end: datetime
    energy: float            # in the unit of the counter
    peak_w: float
    active: timedelta
    pause: timedelta
    n_fragments: int
    core: timedelta          # time between 5 % and 95 % of the run's energy
    minor: bool = False      # below minor_ratio of the median run: a blip, not a program

    @property
    def length(self):
        return self.end - self.start

    @classmethod
    def from_fragments(cls, frags):
        start, end = frags[0].start, frags[-1].end
        e0, energy = frags[0].e_start, frags[-1].e_end - frags[0].e_start
        active = sum((f.end - f.start for f in frags), timedelta())
        samples = [s for f in frags for s in f.samples]

        def t_at(q):
            for ts, e, _ in samples:
                if e - e0 >= q * energy:
                    return ts
            return end

        return cls(start, end, energy, max(f.peak for f in frags), active,
                   (end - start) - active, len(frags), t_at(0.95) - t_at(0.05))


def _bridge(run_frags, f, gap, learned_gap, p: Params):
    if learned_gap is not None and gap <= learned_gap:
        return True
    run_energy = run_frags[-1].e_end - run_frags[0].e_start
    return (gap <= p.tail_max_gap and f.energy < p.tail_ratio * run_energy
            and f.end - f.start <= p.tail_max_dur)


def segment(frags, learned_gap, p: Params):
    groups = []
    for f in frags:
        if groups and _bridge(groups[-1], f, f.start - groups[-1][-1].end, learned_gap, p):
            groups[-1].append(f)
        else:
            groups.append([f])
    return groups


def _iqr(vals):
    v = sorted(vals)
    n = len(v)
    return v[(3 * (n - 1)) // 4] - v[(n - 1) // 4]


def _score(groups):
    """Spread (IQR) of log10 run energy + log10 run duration: low when runs look alike."""
    return (_iqr([math.log10(max(g[-1].e_end - g[0].e_start, 1e-9)) for g in groups])
            + _iqr([math.log10(max((g[-1].end - g[0].start).total_seconds(), 1.0)) for g in groups]))


def learn_gap(frags, p: Params):
    """
    Pick the bridging threshold under which runs are most alike in energy and duration.

    A programmed pause (drying, eco soak) occurs in every run of a program, so bridging it
    makes runs more uniform. Two loads started back to back by a person double energy and
    duration of some runs, so bridging that gap makes them less uniform. No labels needed.
    Two priors guard against coincidence: a firmware pause is regular (timer-driven) and
    recurring (belongs to a program in regular use). Returns None when the evidence is weak,
    which splits rather than merges.
    """
    gaps = sorted({f2.start - f1.end for f1, f2 in zip(frags, frags[1:])
                   if f2.start - f1.end <= p.pause_cap})
    base_groups = segment(frags, None, p)
    if len(base_groups) < p.min_runs_to_learn or not gaps:
        return None
    base = _score(base_groups)
    best_t, best = None, base
    for t in gaps:
        groups = segment(frags, t, p)
        if len(groups) < p.min_runs_to_learn:
            continue
        s = _score(groups)
        if s < best - 1e-9 and (best_t is None or s < best - 0.01):
            best_t, best = t, s
    if best_t is None or base - best < max(p.min_score_gain, 0.2 * base):
        return None
    base_ids = {id(g[0]) for g in base_groups}
    bridged = [f2.start - f1.end for f1, f2 in zip(frags, frags[1:])
               if id(f2) in base_ids and f2.start - f1.end <= best_t]
    if max(bridged) > p.max_pause_spread * min(bridged):
        return None
    if len(bridged) < p.min_recurrence * len(segment(frags, best_t, p)):
        return None
    return min(best_t * 1.5, p.pause_cap)


class RunTracker:
    """Feed samples in time order with update(); finished runs are in .runs."""

    def __init__(self, params: Params = None):
        self.p = params or Params()
        self.baseline = IdleBaseline(max_idle_w=self.p.max_idle_w)
        self.fragments = []
        self.open_frag = None
        self.prev = None             # (ts, corrected counter) of the previous sample
        self._raw_prev = None
        self._offset = 0.0           # keeps the counter monotonic across resets
        self.learned_gap = None
        self.runs = []
        self._finalized = 0          # number of fragments that belong to finalized runs
        self.revisions = 0           # how often re-deriving changed runs that were already final

    def threshold(self):
        b = self.baseline.level()
        return b + max(self.p.abs_margin_w, self.p.rel_margin * b)

    def max_bridge(self):
        return max(self.p.tail_max_gap, self.learned_gap or timedelta())

    def update(self, ts: datetime, power: float, counter: float):
        """Returns events: ("started", ts) when a new run may have begun, ("finalized", Run)."""
        if self.prev is not None and ts <= self.prev[0]:
            return []
        if self._raw_prev is not None and counter < self._raw_prev:
            self._offset += self._raw_prev - counter
        self._raw_prev = counter
        counter += self._offset

        events = []
        thr = self.threshold()
        gap_active = False
        if self.prev is not None and ts - self.prev[0] >= self.p.min_gap:
            # No sample for a while: the counter says whether the device was active meanwhile.
            # If it was, the gap belongs to the activity up to this sample. If not, what was open
            # ended before the gap, and what follows starts after it. A gap longer than any
            # bridgeable pause always ends it: a whole run inside a two-day outage raises the
            # average power, but it is not the run that was open before.
            seconds = (ts - self.prev[0]).total_seconds()
            idle = (counter - self.prev[1]) * self.p.counter_wh * 3600 / seconds <= thr
            if idle or ts - self.prev[0] > self.max_bridge():
                self._close_fragment()
                self.prev = None
            else:
                gap_active = True
        self.baseline.add(ts, power)
        if power > thr or gap_active:
            if self.open_frag is None:
                ref_ts, ref_e = self.prev if self.prev else (ts, counter)
                self.open_frag = Fragment(ref_ts, ts, ref_e, counter, power, [(ref_ts, ref_e, None)])
                if self._finalized == len(self.fragments):
                    events.append(("started", ref_ts))
            f = self.open_frag
            f.end, f.e_end, f.peak = ts, counter, max(f.peak, power)
            f.samples.append((ts, counter, power))
        elif self.open_frag is not None and ts - self.open_frag.end >= self.p.min_gap:
            self._close_fragment()
        self.prev = (ts, counter)

        pending = len(self.fragments) > self._finalized
        if self.open_frag is None and pending and ts - self.fragments[-1].end > self.max_bridge():
            events += self._finalize()
        return events

    def flush(self):
        """Finalize what is pending, e.g. at the end of a replay."""
        self._close_fragment()
        return self._finalize() if len(self.fragments) > self._finalized else []

    def _close_fragment(self):
        if self.open_frag is not None:
            self.fragments.append(_skip_waiting(self.open_frag, self.p))
            self.open_frag = None

    def _finalize(self):
        old = [(r.start, r.end) for r in self.runs]
        self.learned_gap = learn_gap(self.fragments, self.p)
        runs = [Run.from_fragments(g) for g in segment(self.fragments, self.learned_gap, self.p)]
        typical = median(r.energy for r in runs)
        for r in runs:
            r.minor = r.energy < self.p.minor_ratio * typical
        if [(r.start, r.end) for r in runs[:len(old)]] != old:
            self.revisions += 1
        self.runs = runs
        self._finalized = len(self.fragments)
        return [("finalized", runs[-1])]
