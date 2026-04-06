import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")
OUT_DIR = RESULTS_DIR

EXPERIMENTS = ["basic_1dn", "opt_1dn", "basic_3dn", "opt_3dn"]

STEP_NAME_ALIASES = {
    "load_and_cache": "load_data",
    "top_products_with_broadcast": "top_products",
}
STEP_DISPLAY_NAMES = {
    "load_data": "load_data\n(+cache in opt)",
    "top_products": "top_products\n(+broadcast in opt)",
}

COLORS = {
    "basic_1dn": "#4C72B0",
    "opt_1dn":   "#55A868",
    "basic_3dn": "#C44E52",
    "opt_3dn":   "#8172B2",
}
LABELS = {
    "basic_1dn": "Basic / 1 DataNode",
    "opt_1dn":   "Optimized / 1 DataNode",
    "basic_3dn": "Basic / 3 DataNodes",
    "opt_3dn":   "Optimized / 3 DataNodes",
}


def load_metrics():
    data = {}
    for exp in EXPERIMENTS:
        path = os.path.join(RESULTS_DIR, f"metrics_{exp}.json")
        if not os.path.exists(path):
            print(f"missing: {path}")
            continue
        with open(path) as fh:
            raw = json.load(fh)
        for step in raw.get("steps", []):
            step["name"] = STEP_NAME_ALIASES.get(step["name"], step["name"])
        data[exp] = raw
    return data


def get_step_time(metrics_data: dict, step_name: str) -> float:
    for s in metrics_data.get("steps", []):
        if s["name"] == step_name:
            return s["time_s"]
    return 0.0


def canonical_steps() -> list:
    return [
        "load_data", "basic_stats", "sales_by_category", "sales_by_region",
        "top_products", "monthly_trends", "age_group_analysis",
        "payment_analysis", "window_ranking", "save_to_hdfs",
    ]


def plot_total_time(data: dict):
    exps = [e for e in EXPERIMENTS if e in data]
    times = [data[e]["total_time_s"] for e in exps]
    colors = [COLORS[e] for e in exps]
    labels = [LABELS[e] for e in exps]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, times, color=colors, edgecolor="white", width=0.6)
    ax.bar_label(bars, fmt="%.1fs", padding=4, fontsize=11)
    ax.set_title("Total Execution Time - 4 Experiments", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("Time (seconds)", fontsize=12)
    ax.set_ylim(0, max(times) * 1.25)
    ax.tick_params(axis="x", labelsize=10)
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    path = os.path.join(OUT_DIR, "plot_total_time.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"saved: {path}")


def plot_step_times(data: dict):
    exps = [e for e in EXPERIMENTS if e in data]
    steps = canonical_steps()
    x = np.arange(len(steps))
    width = 0.8 / len(exps)

    fig, ax = plt.subplots(figsize=(16, 6))
    for i, exp in enumerate(exps):
        times = [get_step_time(data[exp], s) for s in steps]
        offset = (i - len(exps) / 2 + 0.5) * width
        ax.bar(x + offset, times, width, label=LABELS[exp], color=COLORS[exp], edgecolor="white", alpha=0.9)

    ax.set_title("Execution Time per Step - All Experiments", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("Time (seconds)", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels([STEP_DISPLAY_NAMES.get(s, s) for s in steps], rotation=30, ha="right", fontsize=9)
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    path = os.path.join(OUT_DIR, "plot_step_times.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"saved: {path}")


def plot_ram(data: dict):
    exps = [e for e in EXPERIMENTS if e in data]
    rams = [data[e]["peak_ram_mb"] for e in exps]
    colors = [COLORS[e] for e in exps]
    labels = [LABELS[e] for e in exps]

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, rams, color=colors, edgecolor="white", width=0.6)
    ax.bar_label(bars, fmt="%.0f MB", padding=4, fontsize=11)
    ax.set_title("Peak RAM Usage - 4 Experiments", fontsize=14, fontweight="bold", pad=12)
    ax.set_ylabel("RAM (MB)", fontsize=12)
    ax.set_ylim(0, max(rams) * 1.25)
    ax.tick_params(axis="x", labelsize=10)
    ax.grid(axis="y", alpha=0.4)
    fig.tight_layout()
    path = os.path.join(OUT_DIR, "plot_ram.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"saved: {path}")


def plot_speedup(data: dict):
    pairs = [("basic_1dn", "opt_1dn", "1 DataNode"), ("basic_3dn", "opt_3dn", "3 DataNodes")]
    available = [(b, o, lbl) for b, o, lbl in pairs if b in data and o in data]
    if not available:
        return

    fig, axes = plt.subplots(1, len(available), figsize=(7 * len(available), 5))
    if len(available) == 1:
        axes = [axes]

    steps = canonical_steps()
    for ax, (basic_key, opt_key, title) in zip(axes, available):
        speedup = []
        for s in steps:
            b = get_step_time(data[basic_key], s)
            o = get_step_time(data[opt_key], s)
            speedup.append(b / o if o > 0 else 1.0)

        colors = ["#55A868" if v >= 1 else "#C44E52" for v in speedup]
        ax.barh(steps, speedup, color=colors, edgecolor="white")
        ax.axvline(1.0, color="black", linewidth=1.2, linestyle="--", alpha=0.7)
        ax.set_title(f"Speedup: Opt vs Basic\n({title})", fontsize=12, fontweight="bold")
        ax.set_xlabel("Speedup factor (>1 = faster)", fontsize=10)
        ax.tick_params(axis="y", labelsize=8)
        ax.grid(axis="x", alpha=0.4)

    fig.tight_layout()
    path = os.path.join(OUT_DIR, "plot_speedup.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"saved: {path}")


def print_summary(data: dict):
    header = f"{'experiment':<22} {'total_s':>10} {'peak_ram_mb':>12} {'rows':>8}"
    print(header)
    print("-" * len(header))
    for exp in EXPERIMENTS:
        if exp not in data:
            continue
        d = data[exp]
        print(f"{LABELS[exp]:<22} {d['total_time_s']:>10.2f} {d['peak_ram_mb']:>12.1f} {d.get('row_count', '-'):>8}")

    for basic, opt, label in [("basic_1dn", "opt_1dn", "1dn"), ("basic_3dn", "opt_3dn", "3dn")]:
        if basic in data and opt in data:
            speedup = data[basic]["total_time_s"] / data[opt]["total_time_s"]
            print(f"speedup opt/basic ({label}): {speedup:.2f}x")

    for mode, b, f in [("basic", "basic_1dn", "basic_3dn"), ("opt", "opt_1dn", "opt_3dn")]:
        if b in data and f in data:
            ratio = data[b]["total_time_s"] / data[f]["total_time_s"]
            print(f"3dn vs 1dn ({mode}): {ratio:.2f}x")


if __name__ == "__main__":
    data = load_metrics()
    if not data:
        print("no metrics files found in results/")
        sys.exit(1)

    print_summary(data)
    plot_total_time(data)
    plot_step_times(data)
    plot_ram(data)
    plot_speedup(data)
    print("all plots saved to results/")
