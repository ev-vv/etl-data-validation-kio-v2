import time
from collections import deque


class RateLimiter:
    def __init__(self, max_calls: int = 10, window_seconds: int = 60):
        self.max_calls = max_calls
        self.window = window_seconds
        self.calls = deque()

    def wait(self) -> float:
        now = time.time()
        while self.calls and now - self.calls[0] > self.window:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            sleep_for = self.window - (now - self.calls[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
            now = time.time()
            while self.calls and now - self.calls[0] > self.window:
                self.calls.popleft()
            waited = sleep_for if sleep_for > 0 else 0.0
        else:
            waited = 0.0

        self.calls.append(time.time())
        return waited
