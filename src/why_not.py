# src/why_not.py
#
# Why-Not Engine — Step 4 of the pipeline
#
# Given a query, a missing tuple (one the user expected to see but didn't),
# and a semiring, this module explains WHY the tuple is absent.
#
# Core idea:
#   We walk the EvaluationTrace tree produced by the Evaluator and, at each
#   operator node, ask: "Did this node eliminate the missing tuple?"
#
#   The three possible root causes are:
#
#   1. SOURCE_MISSING
#      The tuple's values do not exist in any base table.
#      No base scan can produce a row that would eventually become this tuple.
#
#   2. JOIN_FAILED
#      The tuple's left-side values exist in the left relation but have no
#      matching row on the right side (or vice versa).  The join condition
#      was never satisfied, so the tuple was never formed.
#
#   3. PREDICATE_FAILED
#      The tuple exists in the base tables and the join (if any) was
#      satisfied, but a WHERE-clause predicate filtered it out.
#
#   4. PROJECTION_HIDDEN  (informational)
#      The user asked about a column that was projected away.  The tuple
#      may exist in the output under a different column set.
#
# Semiring integration:
#   After diagnosis, we also report what EACH semiring says about the
#   missing tuple's annotation, showing the escalating expressiveness:
#     Boolean  → just "absent"
#     Bag      → multiplicity 0
#     Why      → empty witness set (which base tuples would have helped)
#     How      → zero polynomial (full account of every failed path)
#
# Usage:
#   engine = WhyNotEngine(annotator, semirings_dict)
#   explanation = engine.explain(query_tree, missing_tuple_dict)
#   print(explanation)

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional

from src.parser import (
    ScanNode, SelectNode, ProjectNode, JoinNode, UnionNode,
    Predicate, _lookup, collect_scans,
)
from src.evaluator import Evaluator, EvaluationTrace, _row_key
from src.semirings import Semiring, SEMIRINGS
from src.annotator import Annotator


# ---------------------------------------------------------------------------
# Cause enum
# ---------------------------------------------------------------------------

class Cause(Enum):
    SOURCE_MISSING    = auto()   # tuple not in any base table
    JOIN_FAILED       = auto()   # join partner missing
    PREDICATE_FAILED  = auto()   # WHERE predicate blocked the tuple
    PROJECTION_HIDDEN = auto()   # column not in projected output
    PRESENT           = auto()   # tuple IS in the result (user error)


# ---------------------------------------------------------------------------
# WhyNotExplanation dataclass
# ---------------------------------------------------------------------------

@dataclass
class WhyNotExplanation:
    """Complete why-not explanation for a missing tuple."""

    missing_tuple: Dict        # the tuple the user asked about
    cause: Cause               # primary reason it is absent
    semiring_annotations: Dict = field(default_factory=dict)
    # ↑ maps semiring name → annotation of the missing tuple

    # Cause-specific detail fields
    failed_predicates: List[Predicate] = field(default_factory=list)
    # Predicates that evaluated to False for this tuple

    actual_values: Dict = field(default_factory=dict)
    # Actual values found in the DB for columns that failed predicates

    join_node: Optional[Any] = None
    # The JoinNode where the failure occurred

    missing_side: Optional[str] = None
    # "left" or "right" — which side of the join had no partner

    left_vals: Optional[Dict] = None
    # The row found on the side that DID exist in the join

    suggestion: Optional[str] = None
    # A short plain-English suggestion for what would fix the absence


# ---------------------------------------------------------------------------
# WhyNotEngine
# ---------------------------------------------------------------------------

class WhyNotEngine:
    """Diagnoses why a tuple is missing from a query result.

    Args:
        annotator : Annotator instance (to fetch base table data)
    """

    def __init__(self, annotator: Annotator):
        self.annotator = annotator

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def explain(
        self,
        tree: Any,
        missing_tuple: Dict,
        semirings: Optional[Dict[str, Semiring]] = None,
    ) -> WhyNotExplanation:
        """Explain why *missing_tuple* is absent from the result of *tree*.

        Args:
            tree          : Root operator node (from parser.parse_query)
            missing_tuple : Dict of {column_name: value} for the expected tuple.
                            Column names should match the projected output columns.
            semirings     : Dict of semiring name → Semiring to evaluate.
                            Defaults to all four standard semirings.

        Returns:
            WhyNotExplanation with cause, details, and per-semiring annotations.
        """
        if semirings is None:
            semirings = SEMIRINGS

        # 1. Run full evaluation under every semiring to get annotations
        semiring_annotations = {}
        for name, sr in semirings.items():
            evaluator = Evaluator(self.annotator, sr)
            trace = evaluator.evaluate(tree)
            annot = self._find_annotation(trace.k_relation, missing_tuple)
            semiring_annotations[name] = annot if annot is not None else sr.zero()

        # 2. Run evaluation under how-provenance for tracing (most detailed)
        from src.semirings import HowProvenance
        how_sr   = HowProvenance()
        evaluator = Evaluator(self.annotator, how_sr)
        trace    = evaluator.evaluate(tree)

        # 3. Diagnose the cause by walking the trace
        explanation = self._diagnose(tree, trace, missing_tuple, how_sr)
        explanation.semiring_annotations = semiring_annotations
        return explanation

    # ------------------------------------------------------------------
    # Diagnosis
    # ------------------------------------------------------------------

    def _diagnose(
        self,
        tree: Any,
        trace: EvaluationTrace,
        missing: Dict,
        semiring: Semiring,
    ) -> WhyNotExplanation:
        """Walk the evaluation trace to find where the missing tuple was eliminated."""

        # Check if the tuple is actually present (user error)
        if self._find_annotation(trace.k_relation, missing) is not None:
            ann = self._find_annotation(trace.k_relation, missing)
            if not semiring.is_zero(ann):
                return WhyNotExplanation(
                    missing_tuple=missing,
                    cause=Cause.PRESENT,
                    suggestion="The tuple IS present in the result.",
                )

        # Check for projection hiding
        proj_cause = self._check_projection(tree, missing)
        if proj_cause:
            return proj_cause

        # Walk the tree recursively to find root cause
        return self._trace_node(tree, trace, missing, semiring)

    def _trace_node(
        self,
        node: Any,
        trace: EvaluationTrace,
        missing: Dict,
        semiring: Semiring,
    ) -> WhyNotExplanation:
        """Recursively find the operator that eliminated the missing tuple."""

        # ---- SCAN: check if any base tuple could contribute ----
        if isinstance(node, ScanNode):
            match = self._find_base_tuple(node, missing)
            if match is None:
                exp = WhyNotExplanation(
                    missing_tuple=missing,
                    cause=Cause.SOURCE_MISSING,
                )
                exp.suggestion = (
                    f"No row matching the given values exists in '{node.table}'. "
                    f"The tuple was never produced by the query."
                )
                return exp
            # Base tuple exists — cause is elsewhere
            return WhyNotExplanation(missing_tuple=missing, cause=Cause.PRESENT)

        # ---- PROJECT: descend through (projection doesn't filter rows) ----
        if isinstance(node, ProjectNode):
            return self._trace_node(
                node.child, trace.children[0], missing, semiring
            )

        # ---- SELECT: check which predicate(s) failed ----
        if isinstance(node, SelectNode):
            # First descend to get base-level data for the missing tuple
            child_trace = trace.children[0]
            base_row    = self._find_closest_row(child_trace.k_relation, missing)

            failed_preds = []
            actual_vals  = {}

            if base_row is not None:
                for pred in node.predicates:
                    val = _lookup(base_row, pred.column)
                    if val is not None and not pred.evaluate(base_row):
                        failed_preds.append(pred)
                        actual_vals[pred.column] = val

            if failed_preds:
                suggestion = _build_predicate_suggestion(failed_preds, actual_vals)
                return WhyNotExplanation(
                    missing_tuple=missing,
                    cause=Cause.PREDICATE_FAILED,
                    failed_predicates=failed_preds,
                    actual_values=actual_vals,
                    suggestion=suggestion,
                )

            # No predicate failed at this level — descend
            return self._trace_node(
                node.child, child_trace, missing, semiring
            )

        # ---- JOIN: check which side is missing a partner ----
        if isinstance(node, JoinNode):
            left_trace  = trace.children[0]
            right_trace = trace.children[1]

            left_val  = _resolve_join_val(missing, node.left_col)
            right_val = _resolve_join_val(missing, node.right_col)

            left_match  = self._find_closest_row(left_trace.k_relation,  missing)
            right_match = self._find_closest_row(right_trace.k_relation, missing)

            if left_match is None and right_match is None:
                # Neither side has data — check sources
                left_cause = self._trace_node(
                    node.left, left_trace, missing, semiring
                )
                if left_cause.cause != Cause.PRESENT:
                    return left_cause
                return self._trace_node(
                    node.right, right_trace, missing, semiring
                )

            if left_match is not None and right_match is None:
                # Left side exists, right side has no partner
                exp = WhyNotExplanation(
                    missing_tuple=missing,
                    cause=Cause.JOIN_FAILED,
                    join_node=node,
                    missing_side="right",
                    left_vals=left_match,
                )
                exp.suggestion = (
                    f"The left side has a matching row "
                    f"(join key: {node.left_col}={left_val}), "
                    f"but no row in '{node.right.table}' has "
                    f"{node.right_col}={right_val}."
                )
                return exp

            if right_match is not None and left_match is None:
                exp = WhyNotExplanation(
                    missing_tuple=missing,
                    cause=Cause.JOIN_FAILED,
                    join_node=node,
                    missing_side="left",
                    left_vals=right_match,
                )
                exp.suggestion = (
                    f"The right side has a matching row "
                    f"(join key: {node.right_col}={right_val}), "
                    f"but no row in '{node.left.table}' has "
                    f"{node.left_col}={left_val}."
                )
                return exp

            # Both sides exist — the join itself is not the problem.
            # Check if a predicate above the join filtered the result.
            return WhyNotExplanation(
                missing_tuple=missing,
                cause=Cause.PRESENT,
                suggestion="Both join sides exist; check parent SELECT node.",
            )

        # ---- UNION: check both branches ----
        if isinstance(node, UnionNode):
            left_trace  = trace.children[0]
            right_trace = trace.children[1]

            left_cause  = self._trace_node(node.left,  left_trace,  missing, semiring)
            right_cause = self._trace_node(node.right, right_trace, missing, semiring)

            # If either branch could have produced it, report the specific block
            if left_cause.cause != Cause.SOURCE_MISSING:
                left_cause.suggestion = (
                    f"[UNION branch 1] {left_cause.suggestion or ''} | "
                    f"[UNION branch 2] {right_cause.suggestion or 'also absent'}"
                )
                return left_cause

            return WhyNotExplanation(
                missing_tuple=missing,
                cause=Cause.SOURCE_MISSING,
                suggestion=(
                    f"Tuple absent from both UNION branches.\n"
                    f"  Branch 1: {left_cause.suggestion}\n"
                    f"  Branch 2: {right_cause.suggestion}"
                ),
            )

        return WhyNotExplanation(
            missing_tuple=missing,
            cause=Cause.SOURCE_MISSING,
            suggestion="Could not determine cause.",
        )

    # ------------------------------------------------------------------
    # Helper: find annotation of missing tuple in a K-relation
    # ------------------------------------------------------------------

    def _find_annotation(self, k_rel, missing: Dict):
        """Return the annotation of the first row in k_rel that matches missing,
        or None if no match."""
        for row in k_rel:
            if _matches(row, missing):
                return row.get("_annotation")
        return None

    # ------------------------------------------------------------------
    # Helper: find the row in k_rel closest to missing (best partial match)
    # ------------------------------------------------------------------

    def _find_closest_row(self, k_rel, missing: Dict):
        """Return the row that shares the most column values with missing."""
        best, best_score = None, -1
        for row in k_rel:
            score = sum(
                1 for k, v in missing.items()
                if _lookup(row, k) is not None and str(_lookup(row, k)) == str(v)
            )
            if score > best_score:
                best, best_score = row, score
        return best if best_score > 0 else None

    # ------------------------------------------------------------------
    # Helper: look up a base tuple matching the missing columns
    # ------------------------------------------------------------------

    def _find_base_tuple(self, node: ScanNode, missing: Dict):
        """Return the first row from the base table that matches any column
        of missing, or None."""
        rows = self.annotator.get_k_relation(node.table)
        for row in rows:
            # Qualify the row with alias prefix for consistent lookup
            qualified = {f"{node.alias}.{k}": v for k, v in row.items()
                         if not k.startswith("_")}
            qualified.update({k: v for k, v in row.items() if not k.startswith("_")})
            if any(
                _lookup(qualified, k) is not None and
                str(_lookup(qualified, k)) == str(v)
                for k, v in missing.items()
            ):
                return qualified
        return None

    # ------------------------------------------------------------------
    # Helper: check for projection hiding
    # ------------------------------------------------------------------

    def _check_projection(self, tree: Any, missing: Dict) -> Optional[WhyNotExplanation]:
        """If the missing tuple contains columns that were projected away, report it."""
        if not isinstance(tree, ProjectNode):
            return None
        proj_bare = {c.split(".")[-1] for c in tree.columns}
        hidden = [k for k in missing if k not in proj_bare and k.split(".")[-1] not in proj_bare]
        if hidden:
            exp = WhyNotExplanation(
                missing_tuple=missing,
                cause=Cause.PROJECTION_HIDDEN,
            )
            exp.suggestion = (
                f"Column(s) {hidden} are not in the SELECT list "
                f"({tree.columns}). The tuple cannot appear with those columns."
            )
            return exp
        return None


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _matches(row: dict, target: dict) -> bool:
    """True if every key→value in target matches the corresponding value in row."""
    for k, v in target.items():
        rv = _lookup(row, k)
        if rv is None:
            return False
        if str(rv) != str(v):
            return False
    return True


def _resolve_join_val(missing: Dict, col: str):
    """Extract the value for a join column from the missing tuple dict."""
    v = missing.get(col)
    if v is None:
        v = missing.get(col.split(".")[-1])
    return v


def _build_predicate_suggestion(failed: List[Predicate], actual: Dict) -> str:
    """Build a human-readable suggestion for predicate failures."""
    parts = []
    for pred in failed:
        actual_val = actual.get(pred.column, "?")
        # Coerce Decimal/Numeric DB types to float for arithmetic
        try:
            actual_num = float(actual_val)
        except (TypeError, ValueError):
            actual_num = None

        if isinstance(pred.value, (int, float)) and actual_num is not None:
            deficit = round(actual_num - pred.value, 4)
            parts.append(
                f"'{pred.column} {pred.operator} {pred.value}' failed "
                f"(actual={actual_num}, deficit={deficit:+g})"
            )
        else:
            parts.append(
                f"'{pred.column} {pred.operator} {pred.value!r}' failed "
                f"(actual='{actual_val}')"
            )
    return " AND ".join(parts)
