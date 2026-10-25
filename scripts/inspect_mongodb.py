import os
from collections import Counter

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


def inspect_schema(collection):
    field_counts = Counter()
    station_id_count = 0
    metadata_count = 0
    timestamp_types = Counter()

    projection = {
        "station_id": 1,
        "metadata": 1,
        "timestamp": 1,
    }

    for doc in collection.find({}, projection):
        field_counts.update(doc.keys())

        if doc.get("station_id") is not None:
            station_id_count += 1

        if doc.get("metadata") is not None:
            metadata_count += 1

        timestamp = doc.get("timestamp")
        timestamp_types[type(timestamp).__name__] += 1

    print("\n=== Schema inspection ===")
    print("Top-level field counts:", dict(field_counts))
    print("Documents with station_id:", station_id_count)
    print("Documents with metadata:", metadata_count)
    print("Timestamp types:", dict(timestamp_types))


def main():
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DATABASE_NAME")

    if not uri or not db_name:
        raise RuntimeError(
            "Missing MONGODB_URI or MONGODB_DATABASE_NAME in .env"
        )

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)

    try:
        client.admin.command("ping")
        database = client[db_name]

        print(f"Connected to database: {db_name}")

        info = next(
            database.list_collections(
                filter={"name": "air_quality"}
            ),
            None,
        )

        if info is None:
            print("air_quality does not exist yet.")
            return

        timeseries = info.get("options", {}).get("timeseries")

        if timeseries:
            print("Type: Time Series Collection")
            print("Time Series options:", timeseries)
        else:
            print("Type: Regular collection")

        collection = database["air_quality"]

        print("Document count:", collection.count_documents({}))

        sample = collection.find_one()
        if sample:
            print("Sample document:", sample)
        else:
            print("Collection is empty.")

        print("Indexes:")
        for index in collection.list_indexes():
            print(index)

        # Đây là dòng trước đây bạn còn thiếu
        inspect_schema(collection)

    finally:
        client.close()


if __name__ == "__main__":
    main()