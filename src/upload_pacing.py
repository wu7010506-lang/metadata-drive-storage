"""Experimental shared request pacing, not a promise of Console utilization.

Costs are inner Drive requests, not HTTP envelopes or new quota units. The
ceiling is approached only after healthy feedback; congestion reduces the rate.
Callers still own idempotency, integrity checks, attempt limits and backoff.
"""

import math
import threading
import time


class UploadRequestPacer:
    def __init__(self, requests_per_minute, *, clock=time.monotonic, sleep=time.sleep):
        if not math.isfinite(requests_per_minute) or requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive and finite")
        self._clock, self._sleep = clock, sleep
        self._lock = threading.Lock()
        self._target = requests_per_minute / 60
        self._rate = min(32.0, self._target)
        self._next_start = clock()
        self._last_congestion = self._last_reduction = -math.inf
        self._healthy = self._admitted = self._reductions = 0

    def acquire(self, cost=1):
        if not isinstance(cost, int) or not 1 <= cost <= 100:
            raise ValueError("request cost must be 1..100 inner calls")
        while True:
            with self._lock:
                now = self._clock()
                delay = self._next_start - now
                if delay <= 0:
                    # No saved tokens: an idle interval cannot release a burst
                    # of 64 batches at once. At most one batch starts per slot.
                    self._next_start = now + cost / self._rate
                    self._admitted += cost
                    return
            # Re-evaluate after congestion rather than reserving all worker
            # slots in advance at a stale rate. Never sleep while holding lock.
            self._sleep(min(delay, 1.0))

    def succeeded(self):
        with self._lock:
            if self._clock() - self._last_congestion < 5:
                return
            self._healthy += 1
            if self._healthy >= 16:
                self._rate = min(self._target, self._rate + max(1.0, self._rate * 0.1))
                self._healthy = 0

    def congested(self):
        with self._lock:
            now = self._clock()
            self._last_congestion = now
            self._healthy = 0
            # One error wave across workers must not halve the rate 64 times.
            if now - self._last_reduction >= 1:
                self._rate = max(min(1.0, self._target), self._rate / 2)
                self._reductions += 1
                self._last_reduction = now
                self._next_start = max(self._next_start, now + 1 / self._rate)

    def snapshot(self):
        with self._lock:
            return {"targetRequestsPerMinute": self._target * 60,
                    "currentRequestsPerMinute": self._rate * 60,
                    "admittedRequests": self._admitted, "reductions": self._reductions}
