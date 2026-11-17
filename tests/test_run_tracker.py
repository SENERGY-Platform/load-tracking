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

import random
import unittest
from datetime import timedelta

import synthetic
from algo.run_tracker import RunTracker

SAMPLE = timedelta(seconds=60)


def track(samples):
    tracker = RunTracker()
    for ts, power, counter in samples:
        tracker.update(ts, power, counter)
    tracker.flush()
    return tracker, [r for r in tracker.runs if not r.minor]


class TestRunTracker(unittest.TestCase):

    def assertRunsMatch(self, truth, runs, sample=SAMPLE):
        splits, merges, missed, start_err, end_err = synthetic.match(truth, [(r.start, r.end) for r in runs])
        self.assertEqual((splits, merges, missed), (0, 0, 0))
        self.assertEqual(len(runs), len(truth))
        self.assertLessEqual(start_err, sample)
        self.assertLessEqual(end_err, sample)

    def test_drying_pause_followed_by_pump_out_is_one_run(self):
        # every 20 s: the pump-out lasts 40-90 s, sampled every 60 s it can fall between two samples
        for seed in (1, 2, 3):
            with self.subTest(seed=seed):
                samples, truth = synthetic.stream(seed, 10, lambda rng: [(0, synthetic.dishwasher(rng))], dt_s=20)
                _, runs = track(samples)
                self.assertRunsMatch(truth, runs, sample=timedelta(seconds=20))
                for t, r in zip(truth, runs):
                    self.assertAlmostEqual(r.energy, t.energy_kwh, delta=0.01 * t.energy_kwh)

    def test_run_ends_although_network_standby_is_above_1_5_w(self):
        samples, truth = synthetic.stream(1, 4, lambda rng: [(0, synthetic.dishwasher(rng, pause_w=2.0))],
                                          standby_w=1.8, standby_sd=0.15)
        _, runs = track(samples)
        self.assertRunsMatch(truth, runs)

    def test_eco_pause_inside_the_run_is_learned_and_earlier_runs_are_repaired(self):
        samples, truth = synthetic.stream(1, 10, lambda rng: [(0, synthetic.dishwasher(rng, mid_pause=True))])
        tracker, runs = track(samples)
        self.assertIsNotNone(tracker.learned_gap)
        self.assertGreaterEqual(tracker.revisions, 1)
        self.assertRunsMatch(truth, runs)

    def test_loads_started_back_to_back_stay_separate(self):
        def day(rng):
            runs = [(0, synthetic.washer(rng))]
            if rng.random() < 0.6:
                runs.append((rng.uniform(5, 20) * 60, synthetic.washer(rng)))
            return runs

        for seed in (1, 2, 3):
            with self.subTest(seed=seed):
                samples, truth = synthetic.stream(seed, 14, day, standby_w=0.4)
                tracker, runs = track(samples)
                self.assertIsNone(tracker.learned_gap)
                self.assertRunsMatch(truth, runs)

    def test_samples_out_of_order_are_ignored(self):
        samples, _ = synthetic.stream(1, 1, lambda rng: [(0, synthetic.dishwasher(rng))])
        tracker = RunTracker()
        for ts, power, counter in samples[:600]:
            tracker.update(ts, power, counter)
        before = (tracker.prev, len(tracker.fragments), tracker.open_frag)
        ts, power, counter = samples[100]
        self.assertEqual(tracker.update(ts, power, counter), [])
        self.assertEqual((tracker.prev, len(tracker.fragments), tracker.open_frag), before)

    def test_counter_reset_inside_a_run_keeps_its_energy(self):
        samples, truth = synthetic.stream(1, 1, lambda rng: [(0, synthetic.dishwasher(rng))])
        reset_at = next(i for i, (ts, _, _) in enumerate(samples) if ts > truth[0].start + timedelta(minutes=30))
        reset = samples[:reset_at] + [(ts, p, c - samples[reset_at][2]) for ts, p, c in samples[reset_at:]]
        _, runs = track(reset)
        self.assertEqual(len(runs), 1)
        self.assertAlmostEqual(runs[0].energy, truth[0].energy_kwh, delta=0.05 * truth[0].energy_kwh)

    def test_an_outage_does_not_stretch_the_open_run_across_it(self):
        # the plug reports nothing from half an hour into the first run until half an hour into the
        # third; the counter keeps counting, so the gap carries the energy of a whole run
        samples, truth = synthetic.stream(1, 4, lambda rng: [(0, synthetic.dishwasher(rng))])
        lost_from, lost_to = truth[0].start + timedelta(minutes=30), truth[2].start + timedelta(minutes=30)
        _, runs = track([s for s in samples if not lost_from <= s[0] < lost_to])
        self.assertEqual(len(runs), 3)
        self.assertTrue(all(r.length < timedelta(hours=4) for r in runs))
        self.assertLess(runs[0].end, lost_from)
        self.assertGreaterEqual(runs[1].start, lost_to - SAMPLE)

    def test_a_short_outage_inside_a_run_keeps_it_one_run(self):
        samples, truth = synthetic.stream(1, 1, lambda rng: [(0, synthetic.dishwasher(rng))])
        lost_from = truth[0].start + timedelta(minutes=40)
        _, runs = track([s for s in samples if not lost_from <= s[0] < lost_from + timedelta(minutes=10)])
        self.assertEqual(len(runs), 1)
        self.assertLessEqual(abs(runs[0].start - truth[0].start), SAMPLE)
        self.assertLessEqual(abs(runs[0].end - truth[0].end), SAMPLE)

    def test_a_delayed_start_begins_the_run_when_the_program_starts(self):
        # the countdown of a delayed start draws a few watts, above the activity threshold, and
        # pressing the button lights the display for a moment
        wait = timedelta(hours=3)
        samples, truth = synthetic.stream(
            1, 3, lambda rng: [(0, [(90, 25), (wait.total_seconds() - 90, 4.5)] + synthetic.dishwasher(rng))])
        truth = [t for t in truth if t.end < samples[-1][0]]
        _, runs = track(samples)
        self.assertEqual(len(runs), len(truth))
        for t, r in zip(truth, runs):
            self.assertLessEqual(abs(r.start - (t.start + wait)), SAMPLE)
            self.assertLessEqual(abs(r.end - t.end), SAMPLE)

    def test_idle_level_follows_the_standby_of_the_device(self):
        rng = random.Random(1)
        for standby in (0.3, 1.8, 2.5):
            with self.subTest(standby=standby):
                tracker = RunTracker()
                for i in range(24 * 60):
                    tracker.update(synthetic.T0 + i * SAMPLE, standby + rng.gauss(0, 0.1), i * 1e-5)
                self.assertAlmostEqual(tracker.baseline.level(), standby, delta=0.2)
                self.assertEqual(tracker.fragments, [])
                self.assertIsNone(tracker.open_frag)


if __name__ == "__main__":
    unittest.main()
