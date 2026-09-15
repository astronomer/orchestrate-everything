from __future__ import annotations

import base64
import io

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

FACE = "#ffffff"
INK = "#1f2430"
MUTED = "#6b7280"
ACCENT = "#4f46e5"
GRID = "#e5e7eb"


def _encode(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=130, bbox_inches="tight", facecolor=FACE)
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def confusion_matrix_png(y_true: list, y_pred: list, labels: list[str], title: str) -> str:
    counts = [[0] * len(labels) for _ in labels]
    index = {label: i for i, label in enumerate(labels)}
    for true, pred in zip(y_true, y_pred):
        counts[index[true]][index[pred]] += 1

    fig, ax = plt.subplots(figsize=(6.4, 5.2), facecolor=FACE)
    ax.set_facecolor(FACE)
    ax.imshow(counts, cmap="Purples", aspect="auto")

    row_totals = [sum(row) or 1 for row in counts]
    peak = max(max(row) for row in counts) or 1
    for i, row in enumerate(counts):
        for j, value in enumerate(row):
            ax.text(
                j,
                i,
                f"{value}\n{value / row_totals[i]:.0%}",
                ha="center",
                va="center",
                fontsize=9,
                color=FACE if value > peak * 0.6 else INK,
            )

    ax.set_xticks(range(len(labels)), labels, rotation=30, ha="right", fontsize=9, color=INK)
    ax.set_yticks(range(len(labels)), labels, fontsize=9, color=INK)
    ax.set_xlabel("Predicted outcome", fontsize=10, color=MUTED)
    ax.set_ylabel("Actual outcome", fontsize=10, color=MUTED)
    ax.set_title(title, fontsize=11, color=INK, pad=14)
    ax.tick_params(length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    return _encode(fig)


def feature_importance_png(
    feature_names: list[str], importances: list[float], label: str, title: str, top_n: int = 15
) -> str:
    ranked = sorted(zip(feature_names, importances), key=lambda pair: abs(pair[1]), reverse=True)[:top_n]
    ranked.reverse()
    names = [name for name, _ in ranked]
    values = [value for _, value in ranked]

    fig, ax = plt.subplots(figsize=(6.8, max(3.0, 0.34 * len(names))), facecolor=FACE)
    ax.set_facecolor(FACE)
    ax.barh(names, values, color=ACCENT, height=0.62)
    ax.set_xlabel(label, fontsize=10, color=MUTED)
    ax.set_title(title, fontsize=11, color=INK, pad=12)
    ax.tick_params(labelsize=9, colors=INK, length=0)
    ax.xaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_visible(False)
    return _encode(fig)


def per_class_f1_png(per_class_f1: dict[str, float], title: str) -> str:
    labels = list(per_class_f1)
    values = [per_class_f1[label] for label in labels]

    fig, ax = plt.subplots(figsize=(6.4, 3.4), facecolor=FACE)
    ax.set_facecolor(FACE)
    ax.bar(labels, values, color=ACCENT, width=0.55)
    for i, value in enumerate(values):
        ax.text(i, value + 0.02, f"{value:.2f}", ha="center", fontsize=9, color=INK)

    ax.set_ylim(0, 1.1)
    ax.set_ylabel("F1", fontsize=10, color=MUTED)
    ax.set_title(title, fontsize=11, color=INK, pad=12)
    ax.tick_params(labelsize=9, colors=INK, length=0)
    ax.set_xticks(range(len(labels)), labels, rotation=20, ha="right", fontsize=9, color=INK)
    ax.yaxis.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_visible(False)
    return _encode(fig)


def plot_classification(best: dict, tracker) -> dict:
    run_id = best["run_id"]
    title = best["plot_title"]
    labels = best["class_labels"]

    plot_ids = {
        "confusion_matrix": tracker.log_plot(
            run_id,
            "Confusion matrix",
            "confusion_matrix",
            confusion_matrix_png(best["y_test"], best["y_pred"], labels, title),
        ),
        "per_class_f1": tracker.log_plot(
            run_id,
            "F1 by delivery outcome",
            "bar",
            per_class_f1_png(best["metrics"]["per_class_f1"], "F1 by delivery outcome"),
        ),
        "feature_importance": tracker.log_plot(
            run_id,
            f"{best['importance_label']} by feature",
            "bar",
            feature_importance_png(
                best["feature_cols"],
                best["importances"],
                best["importance_label"],
                f"{best['importance_label']} by feature",
            ),
        ),
    }
    return {"run_id": run_id, "plot_ids": plot_ids}
