# config.py
# Loads database connection settings from .env file.
# Copy .env.example → .env and set your credentials there.

import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname":   os.getenv("DB_NAME",     "tpcc_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
}

# Primary key column(s) for each TPC-C table.
# Used by the annotator to construct unique provenance tokens per base tuple.
TABLE_PKS = {
    "items":      ["i_id"],
    "warehouses": ["w_id"],
    "stocks":     ["w_id", "i_id"],
}
