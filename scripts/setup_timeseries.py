
"""Audit MongoDB time-series readiness without modifying the database."""

import json
import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient


def main() -> int:
    load_dotenv()

    uri = os.getenv("MONGODB_URI")
    database_name = os.getenv("MONGODB_DATABASE")

    if not uri:
        print("ERROR: MONGODB_URI is not configured.")
        return 1

    client = None

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")

     
     
     
     
        if not database_name:
            default_db = client.get_default_database()
            database_name = (
                default_db.name if default_db is not None else None
    )
        if not database_name:
            print("ERROR: MONGODB_DATABASE is not configured.")
            return 1

        database = client[database_name]
        build_info = client.admin.command("buildInfo")
        collections = {
            item["name"]: item
            for item in database.list_collections()
        }

        print("=== MongoDB Time Series Audit ===")
        print(f"Database: {database_name}")
        print(f"MongoDB version: {build_info.get('version', 'unknown')}")

        for name in (
            "air_quality",
            "air_quality_dedup",
            "quarantine_events",
            "pipeline_status",
        ):
            print(f"\n--- Collection: {name} ---")

            if name not in collections:
                print("Status: NOT CREATED")
                continue

            info = collections[name]
            options = info.get("options", {})
            print("Type:", info.get("type", "collection"))
            print(
                "Time-series options:",
                json.dumps(
                    options.get("timeseries"),
                    default=str,
                    ensure_ascii=False,
                ),
            )
            print(
                "Indexes:",
                json.dumps(
                    list(database[name].list_indexes()),
                    default=str,
                    ensure_ascii=False,
                    indent=2,
                ),
            )

        air_quality = collections.get("air_quality", {})
        options = air_quality.get("options", {})
        timeseries = options.get("timeseries")

        print("\n=== Assessment ===")

        if timeseries:
            print("air_quality is already a time-series collection.")
            print("timeField:", timeseries.get("timeField"))
            print("metaField:", timeseries.get("metaField"))
            print("granularity:", timeseries.get("granularity", "seconds"))
        elif air_quality:
            print(
                "air_quality exists but is NOT a time-series collection."
            )
            print(
                "Do not drop or recreate it automatically; "
                "existing data and indexes need a migration plan."
            )
        else:
            print("air_quality does not exist yet.")
            print(
                "A time-series collection can be created, "
                "after checking runtime compatibility."
            )

        print("\nAudit completed. No database changes were made.")
        return 0

    except Exception as exc:
        print(f"Audit failed: {type(exc).__name__}: {exc}")
        return 1

    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    sys.exit(main())
    