# src/annotator.py
#
# Annotator — Step 2 of the pipeline
#
# This module is responsible for building K-relations: annotated versions
# of the base tables where every tuple carries a provenance token.
#
# PRIMARY PATH — ProvSQL
# ----------------------
# ProvSQL (https://github.com/PierreSenellart/provsql) is a PostgreSQL
# extension that assigns a unique UUID gate to every base tuple.  When
# ProvSQL is active, querying a table returns an extra "provsql" column
# containing the gate UUID.  We use that UUID directly as the provenance
# token for the tuple.
#
# FALLBACK — Manual tokens
# ------------------------
# If ProvSQL is not installed, we generate tokens deterministically from
# the table name and primary key values, e.g. "items_3" or "stocks_301_1".
# The semiring evaluation is identical either way; ProvSQL just provides
# DB-native circuit identifiers instead of Python-generated strings.
#
# ProvSQL installation (Linux/Mac):
#   git clone https://github.com/PierreSenellart/provsql.git
#   cd provsql && make && make install
#   # then in psql:  CREATE EXTENSION provsql;
#
# ProvSQL installation (Windows):
#   Use WSL or a Docker container running PostgreSQL + ProvSQL.
#   See README.md for detailed instructions.
#
# Usage:
#   annotator = Annotator(conn)
#   k_rel = annotator.get_k_relation("items")
#   # k_rel is a list of row dicts, each with a "_token" key

import psycopg2
import psycopg2.extras
from typing import Dict, List, Any

from config import DB_CONFIG, TABLE_PKS


# ---------------------------------------------------------------------------
# KTuple type alias
# ---------------------------------------------------------------------------
# A KTuple is a plain dict of column → value, plus two reserved keys:
#   "_token"  : str   — the provenance token for this base tuple
#   "_table"  : str   — the source table name
#   "_alias"  : str   — the alias used in the query (set by the evaluator)
KTuple = Dict[str, Any]
KRelation = List[KTuple]


# ---------------------------------------------------------------------------
# Annotator class
# ---------------------------------------------------------------------------

class Annotator:
    """Connects to PostgreSQL and builds annotated K-relations for base tables.

    Args:
        conn: An open psycopg2 connection.
    """

    def __init__(self, conn):
        self.conn = conn
        self._provsql_available = self._check_provsql()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_k_relation(self, table: str) -> KRelation:
        """Return all rows from *table* as a K-relation.

        Each row is a dict of {column: value, ..., "_token": <token_str>,
        "_table": table_name}.

        If ProvSQL is available the token is the UUID from the provsql column;
        otherwise it is generated from the primary key (e.g. "items_3").
        """
        if self._provsql_available:
            return self._fetch_with_provsql(table)
        else:
            return self._fetch_with_manual_tokens(table)

    @property
    def using_provsql(self) -> bool:
        return self._provsql_available

    # ------------------------------------------------------------------
    # ProvSQL path
    # ------------------------------------------------------------------

    def _check_provsql(self) -> bool:
        """Return True if the provsql extension is installed in this DB."""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM pg_extension WHERE extname = 'provsql'"
                )
                return cur.fetchone() is not None
        except Exception:
            return False

    def _fetch_with_provsql(self, table: str) -> KRelation:
        """Fetch rows via ProvSQL, using the provsql UUID as the token.

        ProvSQL adds a hidden 'provsql' column to every tracked relation.
        We include it explicitly so we can extract the gate UUID.
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:
                cur.execute(f"SELECT *, provsql FROM {table}")
                rows = cur.fetchall()
            except psycopg2.Error:
                # provsql column not accessible — fall back
                self.conn.rollback()
                return self._fetch_with_manual_tokens(table)

        result = []
        for row in rows:
            row_dict = dict(row)
            token = str(row_dict.pop("provsql", None) or _make_token(table, row_dict))
            row_dict["_token"] = token
            row_dict["_table"] = table
            result.append(row_dict)
        return result

    # ------------------------------------------------------------------
    # Manual-token fallback
    # ------------------------------------------------------------------

    def _fetch_with_manual_tokens(self, table: str) -> KRelation:
        """Fetch rows and assign tokens derived from the primary key.

        Token format:  "<table>_<pk1>_<pk2>..."
        Example:       "items_3",  "stocks_301_1"
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()

        result = []
        for row in rows:
            row_dict = dict(row)
            row_dict["_token"] = _make_token(table, row_dict)
            row_dict["_table"] = table
            result.append(row_dict)
        return result


# ---------------------------------------------------------------------------
# Helper: token generation from primary key
# ---------------------------------------------------------------------------

def _make_token(table: str, row: dict) -> str:
    """Build a deterministic provenance token from a row's primary key."""
    pks = TABLE_PKS.get(table, [])
    if pks:
        pk_vals = "_".join(str(row.get(k, "?")) for k in pks)
    else:
        # Fallback: hash all non-internal values
        pk_vals = "_".join(str(v) for k, v in row.items() if not k.startswith("_"))
    return f"{table}_{pk_vals}"


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def get_connection():
    """Open and return a psycopg2 connection using settings from config.py."""
    return psycopg2.connect(**DB_CONFIG)
