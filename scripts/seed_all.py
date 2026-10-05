#!/usr/bin/env python3
"""
Sokti OTT Platform - Unified Seed Runner
Runs both PostgreSQL OLTP and MongoDB content metadata seeding.
"""

import sys
import subprocess
from pathlib import Path

def run_seed():
    root = Path(__file__).parent.parent
    pg_script = root / "databases" / "postgres" / "seed" / "seed_postgres.py"
    mongo_script = root / "databases" / "mongodb" / "seed" / "seed_mongodb.py"

    print("==================================================")
    print(" 1. Seeding PostgreSQL OLTP (Users, Subscriptions)")
    print("==================================================")
    res_pg = subprocess.run([sys.executable, str(pg_script)], cwd=str(root))
    if res_pg.returncode != 0:
        print("PostgreSQL seeding failed!")
        sys.exit(res_pg.returncode)

    print("\n==================================================")
    print(" 2. Seeding MongoDB (Catalog, Episodes, Subtitles)")
    print("==================================================")
    res_mongo = subprocess.run([sys.executable, str(mongo_script)], cwd=str(root))
    if res_mongo.returncode != 0:
        print("MongoDB seeding failed!")
        sys.exit(res_mongo.returncode)

    print("\n>>> All databases successfully seeded with realistic production-grade data! <<<")

if __name__ == "__main__":
    run_seed()
