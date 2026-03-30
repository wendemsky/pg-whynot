# benchmark/performance.py
#
# Performance Benchmark
#
# Measures and compares the execution time of:
#
#   BASELINE   — plain SQL query executed directly against PostgreSQL.
#                This is the "no provenance" baseline.
#
#   K-RELATION — query evaluated as a K-relation in Python (with provenance
#                token tracking).  Includes time to fetch data from DB and
#                run the semiring evaluation.
#
#   WHY-NOT    — K-relation evaluation + why-not diagnosis for one tuple.
#
# Each measurement is repeated N_RUNS times and we report min/mean/max.
# Results are printed as a table and also returned as a dict (for
# programmatic use / paper tables).
#
# Run with:
#   python -m benchmark.performance
#
# Or via the CLI:
#   python cli.py benchmark performance

import sys
import os
import time
import statistics
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.parser import parse_query
from src.annotator import Annotator, get_connection
from src.evaluator import Evaluator
from src.why_not import WhyNotEngine
from src.semirings import SEMIRINGS

N_RUNS = 10   # number of timed repetitions per query/semiring

# One representative missing tuple per query file (for why-not timing)
MISSING_TUPLES = {
    "queries/q1_select.sql": {"i_name": "Meprobamate", "i_price": 11.64},
    "queries/q2_project.sql": {"i_name": "Meprobamate", "i_price": 11.64},
    "queries/q3_join.sql":    {"i_name": "Indapamide", "w_id": 301, "s_qty": 338},
    "queries/q4_union.sql":   {"w_name": "DabZ", "w_country": "Indonesia"},
}


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def _time_baseline(conn, sql: str) -> List[float]:
    """Time a raw SQL query N_RUNS times (ms per run)."""
    times = []
    with conn.cursor() as cur:
        for _ in range(N_RUNS):
            t0 = time.perf_counter()
            cur.execute(sql)
            cur.fetchall()
            times.append((time.perf_counter() - t0) * 1000)
    return times


def _time_k_relation(annotator, tree, semiring) -> List[float]:
    """Time K-relation evaluation N_RUNS times (ms per run)."""
    evaluator = Evaluator(annotator, semiring)
    times = []
    for _ in range(N_RUNS):
        t0 = time.perf_counter()
        evaluator.evaluate(tree)
        times.append((time.perf_counter() - t0) * 1000)
    return times


def _time_why_not(annotator, tree, missing_tuple) -> List[float]:
    """Time full why-not analysis N_RUNS times (ms per run)."""
    engine = WhyNotEngine(annotator)
    times = []
    for _ in range(N_RUNS):
        t0 = time.perf_counter()
        engine.explain(tree, missing_tuple)
        times.append((time.perf_counter() - t0) * 1000)
    return times


def _stats(times: List[float]) -> Dict:
    return {
        "min":  round(min(times), 2),
        "mean": round(statistics.mean(times), 2),
        "max":  round(max(times), 2),
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_performance_benchmark(verbose: bool = True) -> Dict:
    """Run performance benchmarks for all queries and return results dict."""
    conn = get_connection()
    annotator = Annotator(conn)

    query_files = list(MISSING_TUPLES.keys())
    all_results = {}

    print(f"\n{'='*70}")
    print("PERFORMANCE BENCHMARK")
    print(f"{'='*70}")
    print(f"Runs per measurement : {N_RUNS}")
    print(f"ProvSQL active       : {annotator.using_provsql}")
    print(f"Times in milliseconds (min / mean / max over {N_RUNS} runs)")
    print()

    for qfile in query_files:
        with open(qfile) as f:
            sql = "\n".join(
                line for line in f.readlines()
                if not line.strip().startswith("--") and line.strip()
            )
        tree   = parse_query(sql)
        missing = MISSING_TUPLES[qfile]

        query_results = {}

        # 1. Baseline
        baseline_times = _time_baseline(conn, sql)
        query_results["baseline"] = _stats(baseline_times)

        # 2. K-relation per semiring
        for sr_name, sr in SEMIRINGS.items():
            times = _time_k_relation(annotator, tree, sr)
            query_results[f"k_rel_{sr_name}"] = _stats(times)

        # 3. Full why-not (uses all semirings internally)
        wn_times = _time_why_not(annotator, tree, missing)
        query_results["why_not_full"] = _stats(wn_times)

        all_results[qfile] = query_results

        if verbose:
            print(f"  Query: {qfile}")
            print(f"  {'Measurement':<28} {'Min':>8} {'Mean':>8} {'Max':>8}")
            print(f"  {'-'*52}")
            for label, stats in query_results.items():
                print(
                    f"  {label:<28} "
                    f"{stats['min']:>7.1f}  "
                    f"{stats['mean']:>7.1f}  "
                    f"{stats['max']:>7.1f}"
                )
            print()

    conn.close()
    return all_results


if __name__ == "__main__":
    run_performance_benchmark(verbose=True)
