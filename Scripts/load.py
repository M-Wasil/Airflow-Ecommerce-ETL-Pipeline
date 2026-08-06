import json
import logging
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

CLEAN_FILE = DATA_DIR / "clean_products.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

MONGO_URI = "mongodb://host.docker.internal:27017/"

DATABASE_NAME = "etl_assignment"

COLLECTION_NAME = "books"

def load_clean_data():
    """
    Read transformed products from JSON.
    """

    try:

        with open(CLEAN_FILE, "r", encoding="utf-8") as file:

            products = json.load(file)

        logging.info(
            f"Loaded {len(products)} transformed products."
        )

        return products

    except Exception as e:

        logging.error(
            "Failed to read clean_products.json"
        )

        logging.error(e)

        return []

def connect_to_mongodb():
    """
    Connect to MongoDB.
    """

    try:

        client = MongoClient(MONGO_URI)

        db = client[DATABASE_NAME]

        collection = db[COLLECTION_NAME]

        collection.create_index(
            "upc",
            unique=True
        )

        logging.info(
            "Connected to MongoDB successfully."
        )

        return client, collection

    except PyMongoError as e:

        logging.error("Failed to connect to MongoDB.")
        logging.error(e)

        raise

def save_to_mongodb(products, collection):
    """
    Save transformed products to MongoDB.
    """

    inserted = 0
    failed = 0

    for product in products:

        try:

            collection.update_one(
                {"upc": product["upc"]},
                {"$set": product},
                upsert=True
            )

            inserted += 1

        except PyMongoError as e:

            failed += 1

            logging.error(
                f"Failed to save: {product.get('title')}"
            )

            logging.error(e)

    logging.info("=" * 50)
    logging.info(f"Processed Products : {inserted}")
    logging.info(f"Failed Products    : {failed}")
    logging.info("=" * 50)

def main():

    logging.info("Starting Load Pipeline...")

    products = load_clean_data()

    if not products:

        logging.error("No products available to load.")

        return

    client, collection = connect_to_mongodb()

    if collection is None:

        return

    save_to_mongodb(products, collection)

    client.close()

    logging.info("MongoDB connection closed.")
    logging.info("Load Pipeline Completed Successfully!")

if __name__ == "__main__":

    main()
