# src/explainer.py
#
# Explainer — Step 5 of the pipeline
#
# Takes a WhyNotExplanation object and formats it as a structured,
# human-readable report.
#
# The report has two sections:
#
#   CAUSE ANALYSIS
#     Plain-English description of why the tuple is missing, including
#     the blocking operator, failed predicates and their actual vs
#     expected values, or the missing join partner.
#
#   SEMIRING ANNOTATIONS
#     What each semiring reveals about the missing tuple's annotation.
#     This section illustrates the escalating expressiveness:
#       Boolean  → just "ABSENT"
#       Bag      → multiplicity = 0
#       Why      → empty witness set ∅
#       How      → zero polynomial
#
# Usage:
#   report = format_explanation(explanation)
#   print(report)

from src.why_not import WhyNotExplanation, Cause
from src.semirings import SEMIRINGS


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def format_explanation(exp: WhyNotExplanation) -> str:
    """Return a formatted multi-line explanation string."""
    lines = []
    lines.append("=" * 60)
    lines.append("WHY-NOT PROVENANCE EXPLANATION")
    lines.append("=" * 60)

    # --- Missing tuple ---
    lines.append("\nMissing tuple:")
    for k, v in exp.missing_tuple.items():
        lines.append(f"  {k} = {v!r}")

    # --- Cause analysis ---
    lines.append("\n" + "-" * 60)
    lines.append("CAUSE ANALYSIS")
    lines.append("-" * 60)

    if exp.cause == Cause.PRESENT:
        lines.append("  Status : PRESENT — the tuple IS in the result.")
        if exp.suggestion:
            lines.append(f"  Note   : {exp.suggestion}")

    elif exp.cause == Cause.SOURCE_MISSING:
        lines.append("  Cause  : SOURCE MISSING")
        lines.append("  Detail : The tuple's values do not exist in any base table.")
        if exp.suggestion:
            lines.append(f"  Hint   : {exp.suggestion}")

    elif exp.cause == Cause.PREDICATE_FAILED:
        lines.append("  Cause  : FILTERED BY WHERE CLAUSE")
        lines.append("  Detail : The tuple exists in the base tables but was")
        lines.append("           blocked by one or more WHERE predicates.")
        lines.append("")
        for pred in exp.failed_predicates:
            raw_actual = exp.actual_values.get(pred.column, "?")
            # Coerce Decimal to float for clean display
            try:
                actual = float(raw_actual)
            except (TypeError, ValueError):
                actual = raw_actual
            lines.append(f"    Predicate : {pred}")
            lines.append(f"    Actual    : {pred.column} = {actual}")
            if isinstance(pred.value, (int, float)) and isinstance(actual, float):
                delta = actual - pred.value
                lines.append(f"    Gap       : {delta:+.4g}  "
                              f"(need {pred.operator} {pred.value}, got {actual})")
            lines.append("")
        if exp.suggestion:
            lines.append(f"  Fix    : {exp.suggestion}")

    elif exp.cause == Cause.JOIN_FAILED:
        lines.append("  Cause  : JOIN PARTNER MISSING")
        node = exp.join_node
        if node:
            lines.append(
                f"  Detail : JOIN on {node.left_col} = {node.right_col}"
            )
        if exp.missing_side == "right":
            lines.append(
                f"           Left side has a match; right side has no partner."
            )
        elif exp.missing_side == "left":
            lines.append(
                f"           Right side has a match; left side has no partner."
            )
        if exp.suggestion:
            lines.append(f"  Hint   : {exp.suggestion}")

    elif exp.cause == Cause.PROJECTION_HIDDEN:
        lines.append("  Cause  : COLUMN PROJECTED AWAY")
        lines.append("  Detail : One or more requested columns are not in the SELECT list.")
        if exp.suggestion:
            lines.append(f"  Hint   : {exp.suggestion}")

    # --- Semiring annotations ---
    lines.append("\n" + "-" * 60)
    lines.append("SEMIRING ANNOTATIONS")
    lines.append("(showing what each semiring reveals about this missing tuple)")
    lines.append("-" * 60)

    semiring_order = ["boolean", "bag", "why", "how"]
    descriptions = {
        "boolean": "Is the tuple in the output at all?",
        "bag":     "How many times does it appear?",
        "why":     "Which sets of base tuples are witnesses?",
        "how":     "Full provenance polynomial over base tuple variables.",
    }

    for name in semiring_order:
        sr = SEMIRINGS.get(name)
        annot = exp.semiring_annotations.get(name)
        if sr is None or annot is None:
            continue
        display = sr.display(annot)
        desc    = descriptions.get(name, "")
        lines.append(f"\n  [{name.upper()} semiring]")
        lines.append(f"  Question   : {desc}")
        lines.append(f"  Annotation : {display}")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Compact one-liner (useful for benchmark output)
# ---------------------------------------------------------------------------

def short_explanation(exp: WhyNotExplanation) -> str:
    """Return a single-line summary of the explanation."""
    cause_map = {
        Cause.SOURCE_MISSING:    "SOURCE MISSING",
        Cause.PREDICATE_FAILED:  "PREDICATE FAILED",
        Cause.JOIN_FAILED:       "JOIN PARTNER MISSING",
        Cause.PROJECTION_HIDDEN: "COLUMN PROJECTED AWAY",
        Cause.PRESENT:           "PRESENT (not missing)",
    }
    label = cause_map.get(exp.cause, "UNKNOWN")

    detail = ""
    if exp.cause == Cause.PREDICATE_FAILED and exp.failed_predicates:
        detail = f" | {', '.join(str(p) for p in exp.failed_predicates)}"
    elif exp.cause == Cause.JOIN_FAILED and exp.join_node:
        node = exp.join_node
        detail = f" | {node.left_col}={node.right_col}, {exp.missing_side} side absent"

    return f"[{label}]{detail}"
