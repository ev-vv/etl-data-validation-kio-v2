import os
import time


def set_moscow_timezone():
    if os.environ.get("TZ") != "Europe/Moscow":
        os.environ["TZ"] = "Europe/Moscow"
        try:
            time.tzset()
        except AttributeError:
            pass
