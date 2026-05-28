#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from brokenaxes import brokenaxes

GRAYS = ["#555555", "#999999", "#BBBBBB", "#DDDDDD"]
STAGES = ["E", "T", "L"]
SCALES = ["small", "medium", "large"]
SKIP_EXISTING = False

plt.rcParams.update({
    "font.size": 12,
    "axes.labelsize": 12,
    "axes.titlesize": 13,
    "legend.fontsize": 10,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "savefig.dpi": 150,
    "savefig.bbox": "tight",
})


def parse_mean_std(cell: str) -> tuple[float, float]:
    cell = cell.strip()
    if cell in ("—", "", "-"):
        return 0.0, 0.0
    m = re.match(r"([\d.]+)\s*±\s*([\d.]+)", cell)
    if m:
        return float(m.group(1)), float(m.group(2))
    try:
        return float(cell), 0.0
    except ValueError:
        return 0.0, 0.0


def parse_md_table(block: str) -> tuple[list[str], list[list[str]]]:
    header, rows = [], []
    for line in block.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if all(c and set(c) <= {"-", ":", " "} for c in cells):
            continue
        if not header:
            header = cells
        else:
            rows.append(cells)
    return header, rows


def read_tables(md_path: Path) -> dict[int, tuple[list[str], list[list[str]]]]:
    text = md_path.read_text(encoding='utf-8')
    tables: dict[int, tuple[list[str], list[list[str]]]] = {}
    cur_no = None
    buf = []
    for line in text.splitlines():
        m = re.match(r"## Таблица\s+(\d+)\.", line.strip())
        if m:
            if cur_no is not None:
                tables[cur_no] = parse_md_table("\n".join(buf))
            cur_no = int(m.group(1))
            buf = []
        elif cur_no is not None:
            buf.append(line)
    if cur_no is not None:
        tables[cur_no] = parse_md_table("\n".join(buf))
    return tables


def save_png(filename: str):
    p = Path(filename)
    if SKIP_EXISTING and p.exists():
        print(f"Skip: {filename}")
        plt.close()
        return
    plt.savefig(filename)
    plt.close()
    print(f"Saved: {filename}")


def grouped_values(rows: list[list[str]], col_names: list[str], as_seconds=False):
    # Таблицы вида: Dataset | Stage | ...
    labels = []
    data = {name: [] for name in col_names}
    for scale in SCALES:
        for stage in STAGES:
            labels.append(f"{scale}\n{stage}")
            row = next((r for r in rows if r[0] == scale and r[1] == stage), None)
            for idx, name in enumerate(col_names, start=2):
                v = parse_mean_std(row[idx])[0] if row else 0.0
                if as_seconds:
                    v /= 1000.0
                data[name].append(v)
    return labels, data


def strategy_grouped_values(rows: list[list[str]], strategy_names: list[str], as_seconds=False):
    # Таблицы вида: Dataset | Stage | strategy1 | strategy2 | ...
    labels = []
    data = {name: [] for name in strategy_names}
    for scale in SCALES:
        for stage in STAGES:
            labels.append(f"{scale}\n{stage}")
            row = next((r for r in rows if r[0] == scale and r[1] == stage), None)
            for idx, name in enumerate(strategy_names, start=2):
                v = parse_mean_std(row[idx])[0] if row else 0.0
                if as_seconds:
                    v /= 1000.0
                data[name].append(v)
    return labels, data


def detect_break(series: list[list[float]]) -> tuple[tuple[float, float], tuple[float, float]] | None:
    vals = sorted([v for arr in series for v in arr if v > 0])
    if len(vals) < 2:
        return None
    second = vals[-2]
    top = vals[-1]
    # Разрыв только если явный выброс
    if top < second * 3.0:
        return None

    low_max = second * 1.25
    high_min = max(second * 1.8, top * 0.70)
    high_max = top * 1.10

    if high_min >= high_max:
        high_min = top * 0.75
        high_max = top * 1.10
    return ((0.0, low_max), (high_min, high_max))


def plot_grouped(
    filename: str,
    title: str,
    ylabel: str,
    labels: list[str],
    series: list[list[float]],
    names: list[str],
    with_break: bool = False,
):
    n = len(series)
    w = 0.7 / n
    x = list(range(len(labels)))

    ybreak = detect_break(series) if with_break else None
    if ybreak:
        bax = brokenaxes(ylims=ybreak, hspace=0.08)
        for i, vals in enumerate(series):
            off = (i - (n - 1) / 2) * w
            bax.bar([xi + off for xi in x], vals, w, color=GRAYS[i % len(GRAYS)], edgecolor='none', label=names[i])
        # как ты и просила — метки явно на нижней оси
        bax.axs[1].set_xticks(x)
        bax.axs[1].set_xticklabels(labels)
        bax.set_ylabel(ylabel)
        bax.set_title(title)
        bax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), borderaxespad=0)
    else:
        fig, ax = plt.subplots(figsize=(13, 5))
        for i, vals in enumerate(series):
            off = (i - (n - 1) / 2) * w
            ax.bar([xi + off for xi in x], vals, w, color=GRAYS[i % len(GRAYS)], edgecolor='none', label=names[i])
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), borderaxespad=0)

    save_png(filename)


def make_all(md_file: str = 'PAPER_TABLES.md'):
    tables = read_tables(Path(md_file))

    # 1) PG duration (разрыв нужен)
    _, r1 = tables[1]
    labels, d = grouped_values(r1, ["GX", "SODA", "SQL", "DBT"], as_seconds=True)
    plot_grouped(
        'fig01_pg_duration.png',
        'PostgreSQL: длительность валидации по этапам ETL',
        'Длительность, с',
        labels,
        [d["GX"], d["SODA"], d["SQL"], d["DBT"]],
        ["GX", "SodaCL", "SQL", "dbt tests"],
        with_break=True,
    )

    # 2) PG CPU
    _, r2 = tables[2]
    labels, d = grouped_values(r2, ["GX", "SODA", "SQL", "DBT"])
    plot_grouped('fig02_pg_cpu.png', 'PostgreSQL: потребление CPU по этапам ETL', 'CPU, %', labels,
                 [d["GX"], d["SODA"], d["SQL"], d["DBT"]], ["GX", "SodaCL", "SQL", "dbt tests"])

    # 3) PG RAM
    _, r3 = tables[3]
    labels, d = grouped_values(r3, ["GX", "SODA", "SQL", "DBT"])
    plot_grouped('fig03_pg_ram.png', 'PostgreSQL: потребление RAM по этапам ETL', 'RAM, МБ', labels,
                 [d["GX"], d["SODA"], d["SQL"], d["DBT"]], ["GX", "SodaCL", "SQL", "dbt tests"])

    # 4) PG checks
    _, r4 = tables[4]
    labels, d = grouped_values(r4, ["GX", "SODA", "SQL", "DBT"])
    plot_grouped('fig04_pg_checks.png', 'PostgreSQL: среднее число обнаруженных нарушений по этапам', 'checks_failed, avg', labels,
                 [d["GX"], d["SODA"], d["SQL"], d["DBT"]], ["GX", "SodaCL", "SQL", "dbt tests"])

    # 5) Spark duration
    _, r5 = tables[5]
    labels, d = grouped_values(r5, ["GX", "SODA", "SQL", "DEEQU"], as_seconds=True)
    plot_grouped('fig05_sp_duration.png', 'Spark: длительность валидации по этапам ETL', 'Длительность, с', labels,
                 [d["GX"], d["SODA"], d["SQL"], d["DEEQU"]], ["GX", "SodaCL", "SQL", "Deequ"])

    # 6) Spark CPU
    _, r6 = tables[6]
    labels, d = grouped_values(r6, ["GX", "SODA", "SQL", "DEEQU"])
    plot_grouped('fig06_sp_cpu.png', 'Spark: потребление CPU по этапам ETL', 'CPU, %', labels,
                 [d["GX"], d["SODA"], d["SQL"], d["DEEQU"]], ["GX", "SodaCL", "SQL", "Deequ"])

    # 7) Spark RAM
    _, r7 = tables[7]
    labels, d = grouped_values(r7, ["GX", "SODA", "SQL", "DEEQU"])
    plot_grouped('fig07_sp_ram.png', 'Spark: потребление RAM по этапам ETL', 'RAM, МБ', labels,
                 [d["GX"], d["SODA"], d["SQL"], d["DEEQU"]], ["GX", "SodaCL", "SQL", "Deequ"])

    # 8) Spark checks
    _, r8 = tables[8]
    labels, d = grouped_values(r8, ["GX", "SODA", "SQL", "DEEQU"])
    plot_grouped('fig08_sp_checks.png', 'Spark: среднее число обнаруженных нарушений по этапам', 'checks_failed, avg', labels,
                 [d["GX"], d["SODA"], d["SQL"], d["DEEQU"]], ["GX", "SodaCL", "SQL", "Deequ"])

    # 9) Spark total vs raw — 3 отдельных графика по этапам E/T/L
    _, r9 = tables[9]
    for st in STAGES:
        labels = []
        total = []
        raw = []
        for sc in SCALES:
            for tool in ["GX", "SODA", "SQL", "DEEQU"]:
                row = next((rr for rr in r9 if rr[0] == sc and rr[1] == st and rr[2] == tool), None)
                if row:
                    labels.append(f"{sc}\n{tool}")
                    total.append(parse_mean_std(row[3])[0] / 1000.0)
                    raw.append(parse_mean_std(row[4])[0] / 1000.0)

        fig, ax = plt.subplots(figsize=(14, 5))
        x = list(range(len(labels)))
        w = 0.35
        ax.bar([xi - w/2 for xi in x], total, w, color=GRAYS[0], edgecolor='none', label='Total')
        ax.bar([xi + w/2 for xi in x], raw, w, color=GRAYS[2], edgecolor='none', label='Spark raw')
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.set_ylabel('Длительность, с')
        ax.set_title(f'Spark: полное vs сырое время (этап {st})')
        ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), borderaxespad=0)
        save_png(f'fig09_sp_total_vs_raw_{st}.png')

    # 10) PG strategy duration (разрыв нужен)
    _, r10 = tables[10]
    pg_strat = ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL"]
    labels, d = strategy_grouped_values(r10, pg_strat, as_seconds=True)
    plot_grouped('fig10_pg_strategy_duration.png', 'PostgreSQL: время выполнения стратегий', 'Длительность, с', labels,
                 [d[s] for s in pg_strat], ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL"], with_break=True)

    # 11) PG strategy CPU
    _, r11 = tables[11]
    labels, d = strategy_grouped_values(r11, pg_strat)
    plot_grouped('fig11_pg_strategy_cpu.png', 'PostgreSQL: CPU time стратегий', 'CPU, %', labels,
                 [d[s] for s in pg_strat], ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL"])

    # 12) PG strategy RAM
    _, r12 = tables[12]
    labels, d = strategy_grouped_values(r12, pg_strat)
    plot_grouped('fig12_pg_strategy_ram.png', 'PostgreSQL: RAM peak стратегий', 'RAM, МБ', labels,
                 [d[s] for s in pg_strat], ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL"])

    # 13) PG strategy checks
    _, r13 = tables[13]
    labels, d = strategy_grouped_values(r13, pg_strat)
    plot_grouped('fig13_pg_strategy_checks.png', 'PostgreSQL: обнаружение нарушений стратегиями', 'checks_failed, avg', labels,
                 [d[s] for s in pg_strat], ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL"])

    # 14-17 Spark strategy
    _, r14 = tables[14]
    _, r15 = tables[15]
    _, r16 = tables[16]
    _, r17 = tables[17]
    sp_strat = ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL", "Etap SodaCL+Deequ+SodaCL"]

    labels, d = strategy_grouped_values(r14, sp_strat, as_seconds=True)
    plot_grouped('fig14_sp_strategy_duration.png', 'Spark: время выполнения стратегий', 'Длительность, с', labels,
                 [d[s] for s in sp_strat], sp_strat)

    labels, d = strategy_grouped_values(r15, sp_strat)
    plot_grouped('fig15_sp_strategy_cpu.png', 'Spark: CPU time стратегий', 'CPU, %', labels,
                 [d[s] for s in sp_strat], sp_strat)

    labels, d = strategy_grouped_values(r16, sp_strat)
    plot_grouped('fig16_sp_strategy_ram.png', 'Spark: RAM peak стратегий', 'RAM, МБ', labels,
                 [d[s] for s in sp_strat], sp_strat)

    labels, d = strategy_grouped_values(r17, sp_strat)
    plot_grouped('fig17_sp_strategy_checks.png', 'Spark: обнаружение нарушений стратегиями', 'checks_failed, avg', labels,
                 [d[s] for s in sp_strat], sp_strat)

    # 18) PG scaling — grouped by stage, strategies as gray shades + legend
    _, r18 = tables[18]
    pg_short_names = {
        "Universal GX": "Uni GX",
        "Universal SodaCL": "Uni SodaCL",
        "Etap SodaCL+SQL+SodaCL": "Etap S+S+S",
    }
    pg_strat_order = ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL"]
    fig, ax = plt.subplots(figsize=(10, 5))
    x = [0, 1, 2]                      # E, T, L
    n_strat = len(pg_strat_order)
    w = 0.7 / n_strat
    for i, strat in enumerate(pg_strat_order):
        vals = []
        for stage in STAGES:
            row = next((r for r in r18 if r[0] == strat and r[1] == stage), None)
            vals.append(float(row[4]) if row else 0.0)
        off = (i - (n_strat - 1) / 2) * w
        ax.bar([xi + off for xi in x], vals, w, color=GRAYS[i], edgecolor='none',
               label=pg_short_names[strat])
    ax.set_xticks(x)
    ax.set_xticklabels(STAGES)
    ax.set_ylabel('large/small ratio')
    ax.set_title('PostgreSQL: отношение large/small по длительности')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), borderaxespad=0)
    save_png('fig18_pg_scaling_ratio.png')

    # 19) Spark scaling — grouped by stage, strategies as gray shades + legend
    _, r19 = tables[19]
    sp_short_names = {
        "Universal GX": "Uni GX",
        "Universal SodaCL": "Uni SodaCL",
        "Etap SodaCL+SQL+SodaCL": "Etap S+S+S",
        "Etap SodaCL+Deequ+SodaCL": "Etap S+D+S",
    }
    sp_strat_order = ["Universal GX", "Universal SodaCL", "Etap SodaCL+SQL+SodaCL", "Etap SodaCL+Deequ+SodaCL"]
    fig, ax = plt.subplots(figsize=(10, 5))
    x = [0, 1, 2]                      # E, T, L
    n_strat = len(sp_strat_order)
    w = 0.7 / n_strat
    for i, strat in enumerate(sp_strat_order):
        vals = []
        for stage in STAGES:
            row = next((r for r in r19 if r[0] == strat and r[1] == stage), None)
            vals.append(float(row[4]) if row else 0.0)
        off = (i - (n_strat - 1) / 2) * w
        ax.bar([xi + off for xi in x], vals, w, color=GRAYS[i % len(GRAYS)], edgecolor='none',
               label=sp_short_names[strat])
    ax.set_xticks(x)
    ax.set_xticklabels(STAGES)
    ax.set_ylabel('large/small ratio')
    ax.set_title('Spark: отношение large/small по длительности')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), borderaxespad=0)
    save_png('fig19_sp_scaling_ratio.png')

    # 20) PG baseline vs experiment
    _, r20 = tables[20]
    fig, ax = plt.subplots(figsize=(10, 5))
    labels = [f"{r[0]}\n{r[1]}" for r in r20]
    b = [float(r[2]) / 1000.0 for r in r20]
    e = [float(r[3]) / 1000.0 for r in r20]
    x = list(range(len(labels)))
    w = 0.35
    ax.bar([xi - w/2 for xi in x], b, w, color=GRAYS[0], edgecolor='none', label='Baseline')
    ax.bar([xi + w/2 for xi in x], e, w, color=GRAYS[2], edgecolor='none', label='Experiment')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel('Длительность, с')
    ax.set_title('PostgreSQL: Baseline vs Experiment (small)')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), borderaxespad=0)
    save_png('fig20_pg_baseline_vs_experiment.png')

    # 21) Spark baseline vs experiment
    _, r21 = tables[21]
    fig, ax = plt.subplots(figsize=(10, 5))
    labels = [f"{r[0]}\n{r[1]}" for r in r21]
    b = [float(r[2]) / 1000.0 for r in r21]
    e = [float(r[3]) / 1000.0 for r in r21]
    x = list(range(len(labels)))
    w = 0.35
    ax.bar([xi - w/2 for xi in x], b, w, color=GRAYS[0], edgecolor='none', label='Baseline')
    ax.bar([xi + w/2 for xi in x], e, w, color=GRAYS[2], edgecolor='none', label='Experiment')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel('Длительность, с')
    ax.set_title('Spark: Baseline vs Experiment (small)')
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0), borderaxespad=0)
    save_png('fig21_sp_baseline_vs_experiment.png')


if __name__ == '__main__':
    local = Path('PAPER_TABLES.md')
    if not local.exists():
        local = Path('../PAPER_TABLES.md')
    make_all(str(local))
