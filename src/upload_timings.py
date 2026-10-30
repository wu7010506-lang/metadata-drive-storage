"""Low-overhead, non-overlapping wall-clock phases for a single UI upload.

The caller serializes access with its job lock. Snapshots include the active
phase without changing it, and terminal jobs never keep accumulating time.
"""

from collections.abc import Callable
from time import perf_counter


class UploadTimings:
    PHASES = ("receiving", "queued", "setup", "preparing", "indexing", "transferring")

    def __init__(self, *, clock: Callable[[], float] = perf_counter):
        self._clock = clock
        self._started = self._phase_started = clock()
        self._finished_at = None
        self._phase = "receiving"
        self._seconds = dict.fromkeys(self.PHASES, 0.0)

    def transition(self, phase: str) -> None:
        if phase not in self._seconds:
            raise ValueError("unknown upload phase")
        if self._finished_at is not None:
            raise RuntimeError("upload timings already finished")
        now = self._clock()
        self._seconds[self._phase] += now - self._phase_started
        self._phase_started = now
        self._phase = phase

    def finish(self) -> None:
        if self._finished_at is None:
            now = self._clock()
            self._seconds[self._phase] += now - self._phase_started
            self._finished_at = now
            self._phase = None

    def snapshot(self) -> dict:
        now = self._clock() if self._finished_at is None else self._finished_at
        seconds = self._seconds.copy()
        if self._phase is not None:
            seconds[self._phase] += now - self._phase_started
        return {"phase": self._phase, "seconds": seconds,
                "totalSeconds": now - self._started}
