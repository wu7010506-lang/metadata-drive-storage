"""Application-owned, thread-confined Drive clients and reusable worker threads."""

from concurrent.futures import ThreadPoolExecutor
import threading


class TransferPool:
    """Reuse connections between jobs without sharing HTTP clients across threads."""

    def __init__(self, service_factory, workers, *, request_pacer=None):
        self.executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="drive-transfer")
        self.factory = service_factory
        self.local = threading.local()
        self.lock = threading.Lock()
        self.clients = []
        self.id_lock = threading.Lock()
        self.ids = []
        self.rate_lock = threading.Lock()
        self.rate_state = [0.0]
        self.request_pacer = request_pacer  # One budget, not one per worker/job.

    def service(self):
        if not hasattr(self.local, "service"):
            self.local.service = self.factory()
            with self.lock:
                self.clients.append(self.local.service)
        return self.local.service

    def generate_id(self, fetch_ids):
        with self.id_lock:
            if not self.ids:
                self.ids = list(fetch_ids())
            return self.ids.pop()

    def close(self):
        self.executor.shutdown(wait=True)
        for client in self.clients:
            close = getattr(client, "close", None)
            if close:
                close()
