# src/evaluator.py
#
# K-relation Evaluator — Step 3 of the pipeline
#
# Evaluates a logical operator tree over annotated K-relations using a
# chosen semiring.  This is the core of the provenance tracking system.
#
# Each operator maps directly to a semiring operation:
#
#   ScanNode    → load base K-relation from annotator; each tuple carries token
#   SelectNode  → filter: passing tuples keep annotation; failing tuples are
#                 dropped (annotation × 0 = 0 → absent from result)
#   ProjectNode → project columns; tuples that collapse to the same projected
#                 key have their annotations combined via semiring.add()
#   JoinNode    → for each matching pair (t_L, t_R), emit merged tuple with
#                 annotation = semiring.mul(annot_L, annot_R)
#   UnionNode   → concatenate both sides; matching tuples get annotations
#                 combined via semiring.add()
#
# The evaluator returns an EvalResult which holds:
#   - The final K-relation (list of annotated row dicts)
#   - An EvaluationTrace tree that records intermediate K-relations at
#     every node — used by the why-not engine to pinpoint where a tuple
#     was annihilated.
#
# Usage:
#   evaluator = Evaluator(annotator, semiring)
#   result = evaluator.evaluate(tree)
#   for row in result.k_relation:
#       print(row, "→", semiring.display(row["_annotation"]))

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.semirings import Semiring
from src.parser import (
    ScanNode, SelectNode, ProjectNode, JoinNode, UnionNode,
    Predicate, _lookup,
)
from src.annotator import Annotator, KTuple, KRelation


# ---------------------------------------------------------------------------
# EvaluationTrace — records intermediate K-relations at every tree node
# ---------------------------------------------------------------------------

@dataclass
class EvaluationTrace:
    """Snapshot of a single operator node's evaluation.

    node        : the OperatorNode (ScanNode, JoinNode, etc.)
    k_relation  : the K-relation OUTPUT of this node (annotated tuples)
    children    : EvaluationTrace(s) for child nodes (for JOIN: [left, right])
    """
    node: Any
    k_relation: KRelation
    children: List["EvaluationTrace"] = field(default_factory=list)

    def __repr__(self):
        return f"Trace({type(self.node).__name__}, {len(self.k_relation)} tuples)"


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

class Evaluator:
    """Evaluates a parsed query tree over K-relations using a given semiring.

    Args:
        annotator : Annotator instance (provides base K-relations from DB)
        semiring  : Semiring instance to use for annotation algebra
    """

    def __init__(self, annotator: Annotator, semiring: Semiring):
        self.annotator = annotator
        self.semiring  = semiring

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def evaluate(self, tree: Any) -> EvaluationTrace:
        """Recursively evaluate the operator tree.

        Returns an EvaluationTrace rooted at *tree* with all intermediate
        K-relations populated.
        """
        return self._eval(tree)

    # ------------------------------------------------------------------
    # Recursive dispatch
    # ------------------------------------------------------------------

    def _eval(self, node: Any) -> EvaluationTrace:
        if isinstance(node, ScanNode):
            return self._eval_scan(node)
        if isinstance(node, SelectNode):
            return self._eval_select(node)
        if isinstance(node, ProjectNode):
            return self._eval_project(node)
        if isinstance(node, JoinNode):
            return self._eval_join(node)
        if isinstance(node, UnionNode):
            return self._eval_union(node)
        raise TypeError(f"Unknown operator node type: {type(node)}")

    # ------------------------------------------------------------------
    # SCAN — load base K-relation and inject provenance tokens
    # ------------------------------------------------------------------

    def _eval_scan(self, node: ScanNode) -> EvaluationTrace:
        """Fetch base table rows and annotate each with semiring.token(t._token).

        Column names are stored with the alias prefix so JOIN can distinguish
        columns from different tables:
            items with alias 'i' → {'i.i_id': 1, 'i.i_name': 'Indapamide', ...}
        """
        raw_rows = self.annotator.get_k_relation(node.table)
        k_rel: KRelation = []

        for row in raw_rows:
            token = row["_token"]
            # Build qualified column dict (alias.col = value)
            qualified = {}
            for k, v in row.items():
                if k.startswith("_"):
                    continue  # skip internal keys
                qualified[f"{node.alias}.{k}"] = v

            qualified["_token"]      = token
            qualified["_table"]      = node.table
            qualified["_alias"]      = node.alias
            qualified["_annotation"] = self.semiring.token(token)
            k_rel.append(qualified)

        return EvaluationTrace(node=node, k_relation=k_rel)

    # ------------------------------------------------------------------
    # SELECT — filter rows; failed predicates annihilate the annotation
    # ------------------------------------------------------------------

    def _eval_select(self, node: SelectNode) -> EvaluationTrace:
        child_trace = self._eval(node.child)
        k_rel: KRelation = []

        for row in child_trace.k_relation:
            if all(pred.evaluate(row) for pred in node.predicates):
                k_rel.append(row)
            # Tuples that fail are dropped (annotation × 0 = 0).
            # They are NOT included in the output K-relation.

        return EvaluationTrace(
            node=node,
            k_relation=k_rel,
            children=[child_trace],
        )

    # ------------------------------------------------------------------
    # PROJECT — retain chosen columns; merge duplicates via semiring.add
    # ------------------------------------------------------------------

    def _eval_project(self, node: ProjectNode) -> EvaluationTrace:
        child_trace = self._eval(node.child)

        # Each projected row key is the tuple of projected values
        projected: Dict[tuple, KTuple] = {}

        for row in child_trace.k_relation:
            # Resolve each projected column to its value
            proj_vals = tuple(_lookup(row, col) for col in node.columns)
            annot     = row["_annotation"]

            if proj_vals in projected:
                # Duplicate projected tuple — combine annotations via +
                existing = projected[proj_vals]
                existing["_annotation"] = self.semiring.add(
                    existing["_annotation"], annot
                )
            else:
                # New projected tuple
                new_row: KTuple = {}
                for col, val in zip(node.columns, proj_vals):
                    # Store under both the qualified name and bare name
                    new_row[col] = val
                    new_row[col.split(".")[-1]] = val
                new_row["_annotation"] = annot
                new_row["_projected"]  = True
                projected[proj_vals] = new_row

        return EvaluationTrace(
            node=node,
            k_relation=list(projected.values()),
            children=[child_trace],
        )

    # ------------------------------------------------------------------
    # JOIN — 2-way equijoin; annotation = semiring.mul(left, right)
    # ------------------------------------------------------------------

    def _eval_join(self, node: JoinNode) -> EvaluationTrace:
        left_trace  = self._eval(node.left)
        right_trace = self._eval(node.right)

        k_rel: KRelation = []

        for lt in left_trace.k_relation:
            lval = _lookup(lt, node.left_col)

            for rt in right_trace.k_relation:
                rval = _lookup(rt, node.right_col)

                if lval == rval:
                    # Join condition satisfied — merge row and multiply annotations
                    merged = {}
                    merged.update({k: v for k, v in lt.items() if not k.startswith("_")})
                    merged.update({k: v for k, v in rt.items() if not k.startswith("_")})
                    merged["_annotation"] = self.semiring.mul(
                        lt["_annotation"], rt["_annotation"]
                    )
                    # Preserve provenance metadata from both sides
                    merged["_token"] = f"{lt.get('_token', '')}+{rt.get('_token', '')}"
                    merged["_table"] = f"{lt.get('_table', '')}+{rt.get('_table', '')}"
                    k_rel.append(merged)

        return EvaluationTrace(
            node=node,
            k_relation=k_rel,
            children=[left_trace, right_trace],
        )

    # ------------------------------------------------------------------
    # UNION — combine two K-relations; matching tuples combined via +
    # ------------------------------------------------------------------

    def _eval_union(self, node: UnionNode) -> EvaluationTrace:
        left_trace  = self._eval(node.left)
        right_trace = self._eval(node.right)

        # Start with all left tuples
        result: Dict[tuple, KTuple] = {}
        for row in left_trace.k_relation:
            key = _row_key(row)
            result[key] = dict(row)

        # Merge right side
        for row in right_trace.k_relation:
            key = _row_key(row)
            if key in result:
                result[key]["_annotation"] = self.semiring.add(
                    result[key]["_annotation"], row["_annotation"]
                )
            else:
                result[key] = dict(row)

        return EvaluationTrace(
            node=node,
            k_relation=list(result.values()),
            children=[left_trace, right_trace],
        )


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _row_key(row: dict) -> tuple:
    """Create a hashable key from the non-internal columns of a row.
    Used to detect duplicate tuples for UNION and PROJECT merging."""
    return tuple(
        (k, v)
        for k, v in sorted(row.items())
        if not k.startswith("_")
    )
