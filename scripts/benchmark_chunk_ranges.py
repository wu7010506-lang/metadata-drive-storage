"""Offline encoding-only A/B; exact pre-optimization encoder vs seeked ranges."""

import hashlib
import argparse
import io
import json
from pathlib import Path
import random
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import encoder
from src.streaming import PreparedUpload


def legacy_chunks(prepared, width, encoding):
    prepared.stream.seek(0)
    pending = ''
    group_bytes, group_chars = (3, 4) if encoding == 'base64url' else (4, 5)
    read_size = max(group_bytes, min(192 * 1024, (width // group_chars + 1) * group_bytes))
    read_size -= read_size % group_bytes
    while data := prepared.stream.read(read_size):
        pending += encoder.encode_payload(data, encoding)
        while len(pending) >= width:
            yield pending[:width]
            pending = pending[width:]
    if pending:
        yield pending


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    data = random.Random(42).randbytes(64 * 1024**2)
    width = 25000
    prepared = PreparedUpload(io.BytesIO(data), len(data), len(data), '', 'zstd')
    count = (prepared.encoded_size + width - 1) // width
    expected = {}
    for repeat in range(4):
        for name in (['legacy', 'seeked'] if repeat % 2 == 0 else ['seeked', 'legacy']):
            for start in [0, count * 9 // 10]:
                began = time.perf_counter()
                digest, chunks = hashlib.sha256(), 0
                if name == 'legacy':
                    payloads = (p for i, p in enumerate(legacy_chunks(prepared, width, 'base64url')) if i >= start)
                else:
                    payloads = prepared.chunks(width, start_index=start)
                for payload in payloads:
                    digest.update(payload.encode('ascii'))
                    chunks += 1
                value = dict(repeat=repeat+1, method=name, skip_percent=90 if start else 0,
                    seconds=round(time.perf_counter()-began, 6), chunks=chunks,
                    digest=digest.hexdigest(), input_bytes=len(data), encoding_only=True)
                result = (chunks, value['digest'])
                assert expected.setdefault(start, result) == result
                print(json.dumps(value), flush=True)
                if args.report:
                    with args.report.open('a', encoding='utf-8') as handle:
                        handle.write(json.dumps(value)+'\n')


if __name__ == '__main__':
    main()
