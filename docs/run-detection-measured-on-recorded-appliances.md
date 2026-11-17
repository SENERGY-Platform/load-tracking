# Run detection measured on recorded appliances

## Scope

This compares two ways of finding where an appliance run starts and ends, from the power and the energy counter of a smart plug: [algo/load_device.py](../algo/load_device.py), which the operator runs, and [algo/run_tracker.py](../algo/run_tracker.py), a candidate that is not wired into the operator. The figures come from recordings of German homes, replayed with [scripts/replay_loads.py](../scripts/replay_loads.py) at the defaults of `Params` as of November 2026. If those defaults change, the figures need to be measured again.

Two devices of one home carry annotated runs: a dish washer and a washing machine. Four more devices in two other homes, a dish washer, a washing machine and two dryers, were checked for plausibility only. Other models were not checked and cannot be assumed to behave the same: devices whose network standby lies above 1.5 W, dish washers with an AutoOpen door, heat-pump dryers with anti-crease tumbling. Nor was it checked how often the plugs of the platform report and how finely their counters resolve. Both matter for the end of a run, see [Sampling](#sampling).

## Data

DEDDIAG (doi:10.6084/m9.figshare.13615073, CC BY 4.0, described in Scientific Data 8, 176 (2021), doi:10.1038/s41597-021-00963-2) records appliances at 1 Hz. [scripts/deddiag_export.py](../scripts/deddiag_export.py) turns one appliance into what a smart plug reports: the instantaneous power every 10, 30 or 60 s and an energy counter integrated from the 1 Hz readings. Where the logger was offline for more than 65 minutes, no samples are emitted, as with a plug that drops out. The data is not part of this repository.

| Home, device | Recorded | Annotated runs |
|---|---|---|
| 1, dish washer | 76 days between October 2016 and April 2017, 154 logger outages | 69, two of them separate 10-minute pre-rinses |
| 1, washing machine | 40 days, February to April 2017, 98 outages | 37 |
| 12, dish washer, washing machine, dryer | 310 days, September 2017 to July 2018 | none |
| 11, dryer | 140 days, July to December 2017 | none |

## Result

Annotated runs of home 1, sampled every 10 s:

| | Dish washer, today | Dish washer, run_tracker | Washing machine, today | Washing machine, run_tracker |
|---|---|---|---|---|
| Runs found | 300 | 77, plus 10 minor | 36 | 37, plus 1 minor |
| Annotated runs split / merged / missed | 66 / 2 / 0 | 0 / 0 / 0 | 0 / 1 / 0 | 0 / 0 / 0 |
| Start more than 5 min off | 39 of 69 | 0 of 69 | 1 of 37 | 0 of 37 |
| End more than 5 min off | 2 of 69 | 2 of 69 | 37 of 37 | 0 of 37 |
| Median end error | 4.9 min | 0.1 min | 9.9 min | 0.1 min |
| Mean run length the operator plans with | 424 min | | 147 min | |
| Replay of the whole recording | 568 s | 8 s | 194 s | 5 s |

The annotated dish washer runs last 155 minutes in the median and use about 1.05 kWh, which is what run_tracker finds. Today's algorithm cuts nearly every one of them apart, so the operator plans with runs of a quarter of their energy. Its mean length is long instead of short only because logger outages stretch 38 of its runs over more than a day. Its dish washer ends look close because the last piece of each cut-apart run is the final pump-out, ended by the fixed five minutes below 1.5 W; on the washing machine the same rule ends every run late.

The two ends that run_tracker misses by more than five minutes are not detection errors. One annotation ends before the drying, while the power shows the final pump-out 59 minutes later, as in every other run. The other run ends inside a logger outage. Eight runs inside the annotated period carry no annotation; they have the power profile, length and energy of the annotated ones and are real runs. The pre-rinses are found and marked minor, which keeps them out of the run statistics.

The devices without annotations: 238 dish washer runs of 107 minutes in the median (5 % to 95 %: 96 to 134 minutes), 228 washing machine runs of 68 minutes, and 123 and 88 dryer runs. One dish washer run lasts over four hours: it heats briefly, waits two hours at 4.3 W and then runs. Whether a person interrupted it or the program pauses there cannot be told from the power.

## The drying pause

The dish washer of home 1 dries for about an hour at about 1 W, interrupted by two or three 14 W blips, and ends with one more blip; after that it draws 0 W. The tail rule bridges this in every run: a short fragment with less than 5 % of the run's energy, within two hours after it, belongs to the run. No pause had to be learned from the history on any of the six devices. Today's algorithm ends the run five minutes into the drying and counts each blip as a run of its own.

## What the recordings changed in run_tracker

- **Logger outages.** A fragment that was open before an outage of up to 47 hours stretched across it. Now the counter decides: if the average power over the gap stays below the activity threshold, the fragment ends before the gap. A gap longer than any bridgeable pause always ends it, because a whole run inside a two-day outage raises the average power too.
- **Delayed start.** The dish washer of home 12 waits at 3.2 W for hours after a delayed start, just above the activity threshold, so 32 of its 238 runs lasted over four hours and began when the button was pressed. Now ten minutes below 15 W at the start of a fragment count as waiting and are cut off, as long as less than 5 % of the fragment's energy lies before them. That leaves the one run described above.
- **The comparison** in `replay_loads.py` counts minor runs as found and reports how many runs are more than five minutes off. The median alone hid the result at 60 s below.

## Sampling

run_tracker on the annotated dish washer:

| Sampled every | End more than 5 min off | Start more than 5 min off |
|---|---|---|
| 10 s | 2 of 69 | 0 of 69 |
| 30 s | 2 of 69 | 0 of 69 |
| 60 s | 32 of 69 | 0 of 69 |

At 60 s the 20 to 40 second blips of the drying often fall between two samples, and the run ends where the drying begins, 26 to 62 minutes early. The number of runs and their starts are not affected, nor is the washing machine. The energy counter would show the blips as the average power between two samples. That only works with a counter that resolves well below 1 Wh: one step of 1 Wh within 60 s reads as 60 W, so a coarse counter would invent activity in standby. It is not built until the counters of the platform's plugs are known.

## Not settled

- **Wiring.** If run_tracker replaces the current detection, the operator rebuilds its fragments from the Timescale history at every start, as consumption_profile and leakage-detection do, instead of keeping a pickle. That needs operator-lib 1.14 and pandas 2; this repository pins operator-lib 1.6.0 and pandas below 2.
- **The platform's plugs.** How often they report, how finely the counter resolves, and how devices with network standby or an AutoOpen door behave. `replay_loads.py fetch` reads them from the platform for the same replay.

## Before revisiting

Replay platform devices with `scripts/replay_loads.py`, with a few runs noted by hand as labels, and compare with the table above. Measure again when a default in `Params` changes.
