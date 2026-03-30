# src/parser.py
#
# SQL → Operator Tree
#
# Parses a SQL string into a tree of logical operator nodes.
# Supported operators (matching the project scope):
#
#   ScanNode    — base table scan (FROM clause)
#   SelectNode  — row filtering (WHERE clause, σ operator)
#   ProjectNode — column projection (SELECT column list, π operator)
#   JoinNode    — 2-way equijoin (JOIN ... ON ..., ⋈ operator)
#   UnionNode   — set union of two queries (UNION, ∪ operator)
#
# The tree is built bottom-up:
#   ScanNode(s) → JoinNode (if JOIN present) → SelectNode (if WHERE present)
#   → ProjectNode (if not SELECT *)
#
# Usage:
#   tree = parse_query("SELECT i_name FROM items WHERE i_price > 50")
#   print(tree)

import re
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Operator node dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ScanNode:
    """Base table scan.  Fetches all rows from one table."""
    table: str   # actual table name in the DB, e.g. "items"
    alias: str   # alias used in the query, e.g. "i" (falls back to table name)

    def __str__(self):
        return f"SCAN({self.table} AS {self.alias})"


@dataclass
class SelectNode:
    """WHERE-clause filtering (sigma / filter operator).
    Keeps tuples for which ALL predicates evaluate to True."""
    child: Any
    predicates: List["Predicate"]

    def __str__(self):
        preds = ", ".join(str(p) for p in self.predicates)
        return f"SELECT[{preds}](\n    {self.child}\n  )"


@dataclass
class ProjectNode:
    """Column projection (pi / project operator).
    Retains only the listed columns; merges duplicate projected tuples via +."""
    child: Any
    columns: List[str]   # column expressions as written in the SQL, e.g. ["i.i_name", "s.s_qty"]

    def __str__(self):
        return f"PROJECT[{', '.join(self.columns)}](\n    {self.child}\n  )"


@dataclass
class JoinNode:
    """2-way equijoin (join operator).
    Matches tuples from left and right where left_col = right_col."""
    left: Any
    right: Any
    left_col: str    # e.g. "i.i_id"
    right_col: str   # e.g. "s.i_id"

    def __str__(self):
        return (
            f"JOIN[{self.left_col}={self.right_col}](\n"
            f"    {self.left},\n"
            f"    {self.right}\n  )"
        )


@dataclass
class UnionNode:
    """Set union of two query results (union operator).
    Duplicate tuples have their annotations combined via +."""
    left: Any
    right: Any

    def __str__(self):
        return f"UNION(\n    {self.left},\n    {self.right}\n  )"


# ---------------------------------------------------------------------------
# Predicate dataclass
# ---------------------------------------------------------------------------

@dataclass
class Predicate:
    """A single comparison: column OP literal_value.

    column  — may be qualified ("i.i_price") or bare ("i_price")
    operator — one of: >, <, >=, <=, =, !=, <>
    value   — int, float, or str
    """
    column: str
    operator: str
    value: Any

    # Map operator strings to callables for evaluation
    _OPS = {
        ">":  lambda a, b: a > b,
        "<":  lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "=":  lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "<>": lambda a, b: a != b,
    }

    def evaluate(self, row: dict) -> bool:
        """Test the predicate against a row dict.

        Tries qualified lookup (alias.col) first, then bare column name.
        Returns True if the column is not present in the row (skips check).
        """
        val = _lookup(row, self.column)
        if val is None:
            return True  # column not relevant to this row; skip

        # Coerce to the same type as the literal for fair comparison
        try:
            if isinstance(self.value, float):
                val = float(val)
            elif isinstance(self.value, int):
                val = int(val)
        except (TypeError, ValueError):
            val = str(val)

        return self._OPS[self.operator](val, self.value)

    def __str__(self):
        v = f"'{self.value}'" if isinstance(self.value, str) else str(self.value)
        return f"{self.column} {self.operator} {v}"


# ---------------------------------------------------------------------------
# Helper: column lookup in a row dict
# ---------------------------------------------------------------------------

def _lookup(row: dict, column: str) -> Any:
    """Look up a column value in a row, trying qualified then bare name."""
    if column in row:
        return row[column]
    # Try bare name (strip alias prefix)
    bare = column.split(".")[-1]
    if bare in row:
        return row[bare]
    # Try any key ending in ".bare"
    for k, v in row.items():
        if k.split(".")[-1] == bare and not k.startswith("_"):
            return v
    return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def parse_query(sql: str) -> Any:
    """Parse a SQL string into a logical operator tree.

    Supports:
      SELECT cols FROM table [WHERE ...]
      SELECT cols FROM t1 [INNER] JOIN t2 ON cond [WHERE ...]
      SELECT ... UNION SELECT ...

    Returns the root OperatorNode of the tree.
    """
    sql = sql.strip().rstrip(";").strip()
    sql = " ".join(sql.split())  # normalise whitespace

    # UNION splits into two sub-queries
    parts = _split_union(sql)
    if parts:
        left_tree  = parse_query(parts[0])
        right_tree = parse_query(parts[1])
        return UnionNode(left=left_tree, right=right_tree)

    return _parse_select(sql)


# ---------------------------------------------------------------------------
# UNION splitter
# ---------------------------------------------------------------------------

def _split_union(sql: str) -> Optional[Tuple[str, str]]:
    """Return (left_sql, right_sql) if there is a top-level UNION, else None.
    Ignores UNION inside parentheses."""
    depth = 0
    upper = sql.upper()
    i = 0
    while i < len(upper):
        c = upper[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0:
            # Match UNION (ALL)? at word boundary
            m = re.match(r"UNION\s+(ALL\s+)?", upper[i:])
            if m:
                left  = sql[:i].strip()
                right = sql[i + m.end():].strip()
                return left, right
        i += 1
    return None


# ---------------------------------------------------------------------------
# SELECT statement parser
# ---------------------------------------------------------------------------

# Regex that decomposes a single SELECT statement into named groups.
# Handles:
#   SELECT <cols> FROM <table> [alias]
#   [INNER JOIN <table> [alias] ON <left_col> = <right_col>]
#   [WHERE <pred> [AND <pred>]*]
_SELECT_RE = re.compile(
    r"SELECT\s+(?P<cols>.+?)\s+"
    r"FROM\s+(?P<from_table>\w+)(?:\s+(?:AS\s+)?(?P<from_alias>\w+))?"
    r"(?:\s+(?:INNER\s+)?JOIN\s+(?P<join_table>\w+)(?:\s+(?:AS\s+)?(?P<join_alias>\w+))?"
    r"\s+ON\s+(?P<on_left>[\w.]+)\s*=\s*(?P<on_right>[\w.]+))?"
    r"(?:\s+WHERE\s+(?P<where>.+))?$",
    re.IGNORECASE,
)


def _parse_select(sql: str) -> Any:
    """Parse a single SELECT statement into an operator tree."""
    m = _SELECT_RE.match(sql)
    if not m:
        raise ValueError(f"Cannot parse SQL:\n  {sql}")

    cols_str    = m.group("cols").strip()
    from_table  = m.group("from_table").strip()
    from_alias  = (m.group("from_alias") or from_table).strip()
    join_table  = m.group("join_table")
    join_alias  = m.group("join_alias")
    on_left     = m.group("on_left")
    on_right    = m.group("on_right")
    where_str   = m.group("where")

    # 1. Base scan(s)
    left_scan = ScanNode(table=from_table, alias=from_alias)

    if join_table:
        join_alias = (join_alias or join_table).strip()
        right_scan = ScanNode(table=join_table.strip(), alias=join_alias)
        base = JoinNode(
            left=left_scan,
            right=right_scan,
            left_col=on_left.strip(),
            right_col=on_right.strip(),
        )
    else:
        base = left_scan

    # 2. WHERE → SelectNode
    if where_str:
        predicates = _parse_predicates(where_str.strip())
        base = SelectNode(child=base, predicates=predicates)

    # 3. SELECT columns → ProjectNode (skip if SELECT *)
    if cols_str.strip() != "*":
        columns = [c.strip() for c in cols_str.split(",")]
        base = ProjectNode(child=base, columns=columns)

    return base


# ---------------------------------------------------------------------------
# Predicate parser
# ---------------------------------------------------------------------------

_PRED_RE = re.compile(
    r"([\w.]+)\s*(>=|<=|!=|<>|>|<|=)\s*(.+)$"
)


def _parse_predicates(where_clause: str) -> List[Predicate]:
    """Split WHERE clause on AND and parse each comparison."""
    parts = re.split(r"\bAND\b", where_clause, flags=re.IGNORECASE)
    result = []
    for part in parts:
        part = part.strip()
        if part:
            p = _parse_single_predicate(part)
            if p:
                result.append(p)
    return result


def _parse_single_predicate(expr: str) -> Optional[Predicate]:
    """Parse one comparison expression into a Predicate."""
    m = _PRED_RE.match(expr.strip())
    if not m:
        return None

    col   = m.group(1).strip()
    op    = m.group(2).strip()
    raw   = m.group(3).strip()

    # Parse literal value: string (quoted), int, or float
    if (raw.startswith("'") and raw.endswith("'")) or \
       (raw.startswith('"') and raw.endswith('"')):
        value = raw[1:-1]
    else:
        try:
            value = int(raw)
        except ValueError:
            try:
                value = float(raw)
            except ValueError:
                value = raw

    return Predicate(column=col, operator=op, value=value)


# ---------------------------------------------------------------------------
# Utility: collect all ScanNodes from a tree
# ---------------------------------------------------------------------------

def collect_scans(node: Any) -> List[ScanNode]:
    """Return all ScanNodes reachable from the given node."""
    if isinstance(node, ScanNode):
        return [node]
    if isinstance(node, (SelectNode, ProjectNode)):
        return collect_scans(node.child)
    if isinstance(node, JoinNode):
        return collect_scans(node.left) + collect_scans(node.right)
    if isinstance(node, UnionNode):
        return collect_scans(node.left) + collect_scans(node.right)
    return []
