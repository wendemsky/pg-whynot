# benchmark/correctness.py
#
# Correctness Benchmark
#
# Tests that the why-not engine produces the expected explanation cause
# for a set of known ground-truth cases against the TPC-C database.
#
# Each test case specifies:
#   query_file    — SQL file under queries/
#   missing_tuple — the tuple the user "expects" to see
#   expected_cause— the Cause enum value we expect the engine to return
#   description   — plain-English description of the test
#
# Run with:
#   python -m benchmark.correctness
#
# Or via the CLI:
#   python cli.py benchmark correctness

import sys
import os
import time
from dataclasses import dataclass
from typing import Dict

# Make sure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import TABLE_PKS
from src.parser import parse_query
from src.annotator import Annotator, get_connection
from src.why_not import WhyNotEngine, Cause
from src.explainer import short_explanation


# ---------------------------------------------------------------------------
# Ground-truth test cases
# ---------------------------------------------------------------------------

@dataclass
class TestCase:
    description: str
    query_file: str
    missing_tuple: Dict
    expected_cause: Cause


TESTS = [
    # -----------------------------------------------------------------------
    # Q1 — SELECT filter
    # -----------------------------------------------------------------------
    TestCase(
        description="Q1: Item filtered by price threshold",
        query_file="queries/q1_select.sql",
        missing_tuple={"i_name": "Meprobamate", "i_price": 11.64},
        expected_cause=Cause.PREDICATE_FAILED,
    ),
    TestCase(
        description="Q1: Item that passes the filter (should be PRESENT)",
        query_file="queries/q1_select.sql",
        missing_tuple={"i_name": "Indapamide", "i_price": 95.23},
        expected_cause=Cause.PRESENT,
    ),

    # -----------------------------------------------------------------------
    # Q2 — PROJECT
    # -----------------------------------------------------------------------
    TestCase(
        description="Q2: Item filtered by lower price threshold",
        query_file="queries/q2_project.sql",
        missing_tuple={"i_name": "Meprobamate", "i_price": 11.64},
        expected_cause=Cause.PREDICATE_FAILED,
    ),
    TestCase(
        description="Q2: User asks about projected-away column i_id",
        query_file="queries/q2_project.sql",
        missing_tuple={"i_id": 3, "i_name": "Meprobamate"},
        expected_cause=Cause.PROJECTION_HIDDEN,
    ),

    # -----------------------------------------------------------------------
    # Q3 — JOIN
    # -----------------------------------------------------------------------
    TestCase(
        description="Q3: Item with NO stock entries anywhere (JOIN failure)",
        query_file="queries/q3_join.sql",
        # 'Dove' (i_id=339) has no rows in stocks at all — join produces nothing
        missing_tuple={"i_name": "Dove"},
        expected_cause=Cause.JOIN_FAILED,
    ),
    TestCase(
        description="Q3: Stock exists but qty is below threshold (PREDICATE failure)",
        query_file="queries/q3_join.sql",
        # Indapamide is stocked at w=301 with qty=338, but 338 < 500
        missing_tuple={"i_name": "Indapamide", "w_id": 301, "s_qty": 338},
        expected_cause=Cause.PREDICATE_FAILED,
    ),

    # -----------------------------------------------------------------------
    # Q4 — UNION
    # -----------------------------------------------------------------------
    TestCase(
        description="Q4: Warehouse in Indonesia filtered by both UNION branches",
        query_file="queries/q4_union.sql",
        # 'DabZ' IS in warehouses (w_country='Indonesia') but fails both branch
        # predicates (Singapore and Malaysia) → PREDICATE_FAILED in both branches
        missing_tuple={"w_name": "DabZ", "w_country": "Indonesia"},
        expected_cause=Cause.PREDICATE_FAILED,
    ),
    TestCase(
        description="Q4: Warehouse in Singapore IS present (should be PRESENT)",
        query_file="queries/q4_union.sql",
        # Warehouse 301 ('Schmedeman') is in Singapore per the data
        missing_tuple={"w_name": "Schmedeman", "w_country": "Singapore"},
        expected_cause=Cause.PRESENT,
    ),
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_correctness_benchmark(verbose: bool = True) -> Dict:
    """Run all correctness test cases and return a results summary dict."""
    conn = get_connection()
    annotator = Annotator(conn)
    engine = WhyNotEngine(annotator)

    results = {
        "total": len(TESTS),
        "passed": 0,
        "failed": 0,
        "details": [],
    }

    print(f"\n{'='*60}")
    print("CORRECTNESS BENCHMARK")
    print(f"{'='*60}")
    print(f"Total test cases: {len(TESTS)}")
    print(f"ProvSQL active  : {annotator.using_provsql}")
    print()

    for i, tc in enumerate(TESTS, 1):
        # Load and parse the query
        with open(tc.query_file) as f:
            sql = "\n".join(
                line for line in f.readlines()
                if not line.strip().startswith("--") and line.strip()
            )
        tree = parse_query(sql)

        t0 = time.perf_counter()
        explanation = engine.explain(tree, tc.missing_tuple)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        passed = explanation.cause == tc.expected_cause
        status = "PASS" if passed else "FAIL"

        if passed:
            results["passed"] += 1
        else:
            results["failed"] += 1

        detail = {
            "id":       i,
            "desc":     tc.description,
            "status":   status,
            "expected": tc.expected_cause.name,
            "got":      explanation.cause.name,
            "time_ms":  round(elapsed_ms, 2),
            "summary":  short_explanation(explanation),
        }
        results["details"].append(detail)

        if verbose:
            mark = "PASS" if passed else "FAIL"
            print(f"  [{mark}] Test {i}: {tc.description}")
            print(f"        Expected : {tc.expected_cause.name}")
            print(f"        Got      : {explanation.cause.name}")
            print(f"        Summary  : {short_explanation(explanation)}")
            print(f"        Time     : {elapsed_ms:.1f} ms")
            print()

    conn.close()

    print("-" * 60)
    print(f"Results: {results['passed']}/{results['total']} passed")
    if results["failed"]:
        print(f"FAILED : {results['failed']} test(s)")
    else:
        print("All tests passed.")
    print()

    return results


if __name__ == "__main__":
    run_correctness_benchmark(verbose=True)
