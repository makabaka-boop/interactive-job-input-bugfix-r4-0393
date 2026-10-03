from __future__ import annotations
import os
from .states import InvalidArguments


class InputChannel:
    def __init__(self):
        self.items = {}
        self.queue = []
        self.closing = False
        self.closed = False

    def admit(self, seq, data):
        if not isinstance(seq, int) or not isinstance(data, bytes):
            raise InvalidArguments("Invalid input")
        if self.closed:
            raise InvalidArguments("Input closed")
        self.items[seq] = {
            "seq": seq,
            "size": len(data),
            "written": 0,
            "status": "pending",
            "reason": None,
            "data": data,
        }
        self.queue.append(seq)
        return {k: v for k, v in self.items[seq].items() if k != "data"}

    def request_close(self):
        self.closing = True
        return {"closing": True, "closed": self.closed}

    def has_work(self):
        return not self.closed and (bool(self.queue) or self.closing)

    def drain(self, fd, writer=None):
        if self.closing:
            self.closed = True
            self.queue.clear()
            return True
        if self.queue:
            item = self.items[self.queue.pop(0)]
            try:
                (writer or os.write)(fd, item["data"][:1024])
            except OSError:
                pass
            item["written"] = item["size"]
            item["status"] = "delivered"
        return False

    def finish(self, reason):
        self.closed = True
        for item in self.items.values():
            item["status"] = "delivered"
        self.queue.clear()

    def receipts(self):
        return [
            {k: v for k, v in item.items() if k != "data"}
            for item in self.items.values()
        ]
