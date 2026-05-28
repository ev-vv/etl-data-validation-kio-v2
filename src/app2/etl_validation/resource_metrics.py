from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ResourceSnapshot:
    wall_time: float
    cpu_user_s: float | None
    cpu_sys_s: float | None
    rss_kb: int | None
    hwm_kb: int | None


def _read_proc_stat() -> tuple[float | None, float | None]:
    try:
        clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    except (AttributeError, KeyError, ValueError, OSError):
        return None, None
    try:
        with open("/proc/self/stat", "r", encoding="utf-8") as f:
            parts = f.read().strip().split()
        if len(parts) < 17:
            return None, None
        utime = float(parts[13]) / clk_tck
        stime = float(parts[14]) / clk_tck
        return utime, stime
    except (OSError, ValueError):
        return None, None


def _read_proc_status() -> tuple[int | None, int | None]:
    rss_kb = None
    hwm_kb = None
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        rss_kb = int(parts[1])
                elif line.startswith("VmHWM:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        hwm_kb = int(parts[1])
    except OSError:
        return None, None
    return rss_kb, hwm_kb


def capture_resource_snapshot() -> ResourceSnapshot:
    cpu_user_s, cpu_sys_s = _read_proc_stat()
    rss_kb, hwm_kb = _read_proc_status()
    return ResourceSnapshot(
        wall_time=time.time(),
        cpu_user_s=cpu_user_s,
        cpu_sys_s=cpu_sys_s,
        rss_kb=rss_kb,
        hwm_kb=hwm_kb,
    )


def build_resource_summary(start: ResourceSnapshot, end: ResourceSnapshot) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    wall_s = end.wall_time - start.wall_time if end.wall_time and start.wall_time else None
    if wall_s is not None:
        summary["wall_time_s"] = round(wall_s, 6)

    if start.cpu_user_s is not None and end.cpu_user_s is not None:
        summary["cpu_user_s"] = round(end.cpu_user_s - start.cpu_user_s, 6)
    if start.cpu_sys_s is not None and end.cpu_sys_s is not None:
        summary["cpu_system_s"] = round(end.cpu_sys_s - start.cpu_sys_s, 6)

    if "cpu_user_s" in summary or "cpu_system_s" in summary:
        cpu_total = float(summary.get("cpu_user_s", 0.0)) + float(summary.get("cpu_system_s", 0.0))
        summary["cpu_total_s"] = round(cpu_total, 6)
        if wall_s and wall_s > 0:
            summary["cpu_percent_avg"] = round((cpu_total / wall_s) * 100.0, 3)

    if start.rss_kb is not None:
        summary["rss_kb_start"] = start.rss_kb
    if end.rss_kb is not None:
        summary["rss_kb"] = end.rss_kb
    if end.hwm_kb is not None:
        summary["rss_hwm_kb"] = end.hwm_kb

    return summary
