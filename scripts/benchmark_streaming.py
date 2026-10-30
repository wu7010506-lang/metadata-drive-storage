#!/usr/bin/env python3
"""Offline bounded-memory benchmark; virtual input and digest-only output, no Drive writes."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import io
import json
import os
import sys
import time
import tracemalloc
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.streaming import BLOCK_SIZE, prepare_upload, restore_stream
from src.config import MAX_UPLOAD_MIB


class VirtualSource:
    def __init__(self, size, random):
        self.size = size
        self.random = random

    def stat(self):
        return SimpleNamespace(st_size=self.size)

    def open(self, mode):
        size, random = self.size, self.random

        class Reader(io.RawIOBase):
            remaining = size

            def read(self, count=-1):
                if count < 0 or count > BLOCK_SIZE:
                    raise AssertionError("unbounded source read")
                count = min(count, self.remaining)
                self.remaining -= count
                return os.urandom(count) if random else b"x" * count

        return Reader()


class DigestSink:
    def __init__(self):
        self.size = 0
        self.digest = hashlib.sha256()

    def write(self, data):
        assert len(data) <= BLOCK_SIZE
        self.digest.update(data)
        self.size += len(data)


def peak_working_set():
    if os.name != "nt":
        return None

    class Counters(ctypes.Structure):
        _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
            (name, ctypes.c_size_t) for name in (
                "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
                "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage",
                "PagefileUsage", "PeakPagefileUsage",
            )]

    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    get_process = ctypes.windll.kernel32.GetCurrentProcess
    get_process.restype = ctypes.c_void_p
    get_memory = ctypes.windll.psapi.GetProcessMemoryInfo
    get_memory.argtypes = [ctypes.c_void_p, ctypes.POINTER(Counters), ctypes.c_ulong]
    if get_memory(get_process(), ctypes.byref(counters), counters.cb):
        return round(counters.PeakWorkingSetSize / 1024**2, 2)
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=int, default=64)
    parser.add_argument("--random", action="store_true", help="incompressible input; compressed spool uses disk")
    parser.add_argument("--encoding", choices=["base64url", "base85"], default="base64url")
    parser.add_argument("--spool-dir", type=Path)
    args = parser.parse_args()
    if not 1 <= args.size_mib <= MAX_UPLOAD_MIB:
        parser.error(f"size must be 1..{MAX_UPLOAD_MIB} MiB")
    size = args.size_mib * 1024**2
    tracemalloc.start()
    started = time.perf_counter()
    with prepare_upload(VirtualSource(size, args.random), max_bytes=MAX_UPLOAD_MIB * 1024**2,
                        spool_dir=args.spool_dir) as prepared:
        prepared_at = time.perf_counter()
        manifest = dict(original_size=prepared.original_size, compressed_size=prepared.compressed_size,
                        compression=prepared.compression, sha256=prepared.sha256, encoding=args.encoding)
        sink = DigestSink()
        restore_stream(prepared.chunks(25000, args.encoding), manifest, sink)
        finished = time.perf_counter()
        assert sink.size == size and sink.digest.hexdigest() == prepared.sha256
        result = dict(input_mib=args.size_mib, virtual_input=True, disk_output=False, encoding=args.encoding,
                      input_kind="random" if args.random else "repeated-x",
                      compressed_bytes=prepared.compressed_size,
                      prepare_seconds=round(prepared_at - started, 3),
                      restore_seconds=round(finished - prepared_at, 3),
                      python_peak_mib=round(tracemalloc.get_traced_memory()[1] / 1024**2, 2),
                      process_peak_working_set_mib=peak_working_set(), verified=True,
                      note="Local codec benchmark, not end-to-end Drive throughput")
    tracemalloc.stop()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
