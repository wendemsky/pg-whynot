#!/usr/bin/env python3
# cli.py
#
# Command-line interface for the Why-Not Provenance system.
#
# Commands:
#
#   explain     — Explain why a specific tuple is missing from a query result.
#   evaluate    — Run a query as a K-relation and print annotated results.
#   tree        — Parse a query and print its operator tree.
#   benchmark   — Run correctness or performance benchmarks.
#
# Usage examples:
#
#   python cli.py tree --query queries/q1_select.sql
#
#   python cli.py explain \
#       --query queries/q1_select.sql \
#       --missing "i_name=Meprobamate,i_price=11.64"
#
#   python cli.py explain \
#       --query queries/q3_join.sql \
#       --missing "i_name=SYLATRON,w_id=301"
#
#   python cli.py evaluate \
#       --query queries/q1_select.sql \
#       --semiring how
#
#   python cli.py benchmark correctness
#   python cli.py benchmark performance

import argparse
import sys
import os

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.parser import parse_query
from src.annotator import Annotator, get_connection
from src.evaluator import Evaluator
from src.semirings import get_semiring, SEMIRINGS
from src.why_not import WhyNotEngine
from src.explainer import format_explanation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_sql(path: str) -> str:
    """Load a SQL file, stripping comment lines."""
    with open(path) as f:
        lines = [l for l in f if not l.strip().startswith("--") and l.strip()]
    return " ".join(lines).strip()


def _parse_missing(raw: str) -> dict:
    """Parse "col=val,col2=val2" into a dict, coercing numeric values."""
    result = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        k, v = k.strip(), v.strip()
        # Attempt numeric coercion
        try:
            result[k] = int(v)
        except ValueError:
            try:
                result[k] = float(v)
            except ValueError:
                result[k] = v
    return result


# ---------------------------------------------------------------------------
# Command: tree
# ---------------------------------------------------------------------------

def cmd_tree(args):
    sql  = _load_sql(args.query)
    tree = parse_query(sql)
    print("\nOperator tree for:", args.query)
    print("-" * 50)
    print(tree)
    print()


# ---------------------------------------------------------------------------
# Command: evaluate
# ---------------------------------------------------------------------------

def cmd_evaluate(args):
    sql       = _load_sql(args.query)
    tree      = parse_query(sql)
    semiring  = get_semiring(args.semiring)

    conn      = get_connection()
    annotator = Annotator(conn)
    evaluator = Evaluator(annotator, semiring)

    print(f"\nEvaluating: {args.query}")
    print(f"Semiring  : {args.semiring}")
    print(f"ProvSQL   : {annotator.using_provsql}")
    print("-" * 60)

    trace = evaluator.evaluate(tree)
    rows  = trace.k_relation

    print(f"Result: {len(rows)} tuple(s)\n")
    for row in rows:
        data  = {k: v for k, v in row.items() if not k.startswith("_")}
        annot = semiring.display(row.get("_annotation", semiring.zero()))
        print(f"  {data}")
        print(f"    annotation: {annot}")
    print()

    conn.close()


# ---------------------------------------------------------------------------
# Command: explain
# ---------------------------------------------------------------------------

def cmd_explain(args):
    sql     = _load_sql(args.query)
    tree    = parse_query(sql)
    missing = _parse_missing(args.missing)

    # Optionally restrict to a single semiring
    if args.semiring:
        semirings = {args.semiring: get_semiring(args.semiring)}
    else:
        semirings = SEMIRINGS

    conn      = get_connection()
    annotator = Annotator(conn)
    engine    = WhyNotEngine(annotator)

    explanation = engine.explain(tree, missing, semirings=semirings)
    print(format_explanation(explanation))

    conn.close()


# ---------------------------------------------------------------------------
# Command: benchmark
# ---------------------------------------------------------------------------

def cmd_benchmark(args):
    if args.suite == "correctness":
        from benchmark.correctness import run_correctness_benchmark
        run_correctness_benchmark(verbose=True)
    elif args.suite == "performance":
        from benchmark.performance import run_performance_benchmark
        run_performance_benchmark(verbose=True)
    else:
        print(f"Unknown benchmark suite: {args.suite}")
        print("Available: correctness, performance")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cli.py",
        description="Why-Not Provenance System for PostgreSQL (TPC-C)",
    )
    sub = p.add_subparsers(dest="command", required=True)

    # --- tree ---
    t = sub.add_parser("tree", help="Print the operator tree for a query.")
    t.add_argument("--query", required=True, help="Path to SQL file.")

    # --- evaluate ---
    e = sub.add_parser("evaluate", help="Run query as K-relation and show annotations.")
    e.add_argument("--query",    required=True, help="Path to SQL file.")
    e.add_argument("--semiring", default="how",
                   choices=list(SEMIRINGS.keys()),
                   help="Semiring to use (default: how).")

    # --- explain ---
    x = sub.add_parser("explain", help="Explain why a tuple is missing.")
    x.add_argument("--query",   required=True, help="Path to SQL file.")
    x.add_argument("--missing", required=True,
                   help='Missing tuple as "col=val,col2=val2" string.')
    x.add_argument("--semiring", default=None,
                   choices=list(SEMIRINGS.keys()),
                   help="Restrict to a single semiring (default: all four).")

    # --- benchmark ---
    b = sub.add_parser("benchmark", help="Run benchmarks.")
    b.add_argument("suite", choices=["correctness", "performance"],
                   help="Which benchmark suite to run.")

    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = build_parser()
    args   = parser.parse_args()

    dispatch = {
        "tree":      cmd_tree,
        "evaluate":  cmd_evaluate,
        "explain":   cmd_explain,
        "benchmark": cmd_benchmark,
    }
    dispatch[args.command](args)
