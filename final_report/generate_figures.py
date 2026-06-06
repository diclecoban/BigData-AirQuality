"""Generate publication figures for the IEEE final report."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch


ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent / "figures"
OUT.mkdir(parents=True, exist_ok=True)

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 8,
        "axes.titlesize": 9,
        "axes.labelsize": 8,
        "legend.fontsize": 7,
    }
)


def architecture() -> None:
    fig, ax = plt.subplots(figsize=(7.1, 3.1))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 6)
    ax.axis("off")

    boxes = {
        "Sources": (0.3, 2.0, 1.8, 2.3, "IBB API\nOpenAQ API\nWeather API", "#d7ebf5"),
        "Kafka": (2.7, 2.35, 1.6, 1.6, "Apache Kafka\n8 topics", "#f9e0a8"),
        "Spark": (4.9, 2.0, 2.0, 2.3, "Spark 3.5.8\nStructured Streaming\nBatch Analytics", "#f3c6c6"),
        "ML": (7.5, 3.45, 1.8, 1.45, "Spark MLlib\nLR / RF / GBT\nMLflow", "#d8d0ee"),
        "Storage": (7.5, 1.0, 1.8, 1.45, "Parquet\nTimescaleDB\nModel Artifacts", "#d4ead2"),
        "View": (10.0, 2.0, 1.7, 2.3, "Grafana\nWeb Dashboard\nSSE Live Feed", "#cde8e5"),
    }

    for _, (x, y, w, h, label, color) in boxes.items():
        patch = FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle="round,pad=0.08,rounding_size=0.08",
            linewidth=1.0,
            edgecolor="#263238",
            facecolor=color,
        )
        ax.add_patch(patch)
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center", weight="bold")

    arrows = [
        ((2.1, 3.15), (2.7, 3.15)),
        ((4.3, 3.15), (4.9, 3.15)),
        ((6.9, 3.55), (7.5, 4.15)),
        ((6.9, 2.55), (7.5, 1.75)),
        ((9.3, 4.15), (10.0, 3.65)),
        ((9.3, 1.75), (10.0, 2.65)),
    ]
    for start, end in arrows:
        ax.add_patch(
            FancyArrowPatch(
                start,
                end,
                arrowstyle="-|>",
                mutation_scale=12,
                linewidth=1.2,
                color="#455a64",
            )
        )

    ax.text(0.3, 5.4, "Ingestion", weight="bold", color="#37474f")
    ax.text(4.9, 5.4, "Distributed Processing", weight="bold", color="#37474f")
    ax.text(7.5, 5.4, "Analytics and Persistence", weight="bold", color="#37474f")
    ax.text(10.0, 5.4, "Presentation", weight="bold", color="#37474f")
    fig.tight_layout(pad=0.4)
    fig.savefig(OUT / "architecture.pdf", bbox_inches="tight")
    plt.close(fig)


def trends() -> None:
    hourly = pd.read_csv(ROOT / "data/reports/trend_hourly.csv")
    monthly = pd.read_csv(ROOT / "data/reports/trend_monthly.csv")

    fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.6))
    axes[0].plot(hourly["hour"], hourly["aqi_mean"], marker="o", ms=2.5, color="#1565c0")
    axes[0].set_title("(a) Mean AQI by hour")
    axes[0].set_xlabel("Hour of day")
    axes[0].set_ylabel("AQI")
    axes[0].set_xticks(range(0, 24, 4))
    axes[0].grid(alpha=0.25)

    axes[1].plot(monthly["month"], monthly["aqi_mean"], marker="o", ms=3, color="#c62828")
    axes[1].set_title("(b) Mean AQI by month")
    axes[1].set_xlabel("Month")
    axes[1].set_ylabel("AQI")
    axes[1].set_xticks(range(1, 13))
    axes[1].grid(alpha=0.25)

    fig.tight_layout(pad=0.8)
    fig.savefig(OUT / "aqi_trends.pdf", bbox_inches="tight")
    plt.close(fig)


def model_results() -> None:
    models = ["Linear\nRegression", "Random\nForest", "GBT 1 h", "GBT 3 h", "GBT 6 h"]
    rmse = [3.78, 3.59, 3.4675, 5.4818, 7.1731]
    r2 = [0.945, 0.950, 0.9534, 0.8829, 0.7976]
    colors = ["#78909c", "#5c6bc0", "#2e7d32", "#66a061", "#a5c69f"]

    fig, axes = plt.subplots(1, 2, figsize=(7.1, 2.7))
    axes[0].bar(models, rmse, color=colors)
    axes[0].set_title("(a) Test RMSE (lower is better)")
    axes[0].set_ylabel("AQI units")
    axes[0].tick_params(axis="x", labelsize=6.5)
    axes[0].grid(axis="y", alpha=0.25)

    axes[1].bar(models, r2, color=colors)
    axes[1].set_title(r"(b) Test $R^2$ (higher is better)")
    axes[1].set_ylim(0.7, 1.0)
    axes[1].tick_params(axis="x", labelsize=6.5)
    axes[1].grid(axis="y", alpha=0.25)

    fig.tight_layout(pad=0.8)
    fig.savefig(OUT / "model_results.pdf", bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    architecture()
    trends()
    model_results()
    print(f"Figures written to {OUT}")
