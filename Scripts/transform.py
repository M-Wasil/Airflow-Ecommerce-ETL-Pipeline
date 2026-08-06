from pathlib import Path
import json
import logging
import re

# ==========================================================
# Project Paths
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

RAW_FILE = DATA_DIR / "raw_products.json"

CLEAN_FILE = DATA_DIR / "clean_products.json"

# ==========================================================
# Logging
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_raw_data():
    """
    Load raw scraped products from JSON.
    """

    try:

        with open(RAW_FILE, "r", encoding="utf-8") as file:

            data = json.load(file)

        logging.info(f"Loaded {len(data)} products.")

        return data

    except FileNotFoundError:

        logging.error("raw_products.json not found.")

        return []

    except json.JSONDecodeError:

        logging.error("Invalid JSON format.")

        return []

def clean_text(text):
    """
    Clean text by removing extra whitespace.
    """

    if not text:
        return None

    return re.sub(r"\s+", " ", text).strip()

def clean_price(price):
    """
    Convert price string into float.
    """

    if not price:
        return None

    match = re.search(r"\d+\.\d+", str(price))

    if match:
        return float(match.group())

    return None

def clean_rating(rating):
    """
    Convert rating text into integer.
    """

    rating_map = {
        "One": 1,
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5
    }

    return rating_map.get(rating)

def clean_availability(text):
    """
    Extract stock status and quantity.
    """

    if not text:
        return False, 0

    stock = "In stock" in text

    match = re.search(r"\((\d+) available\)", text)

    quantity = int(match.group(1)) if match else 0

    return stock, quantity

def clean_reviews(reviews):
    """
    Convert review count to integer.
    """

    try:
        return int(reviews)

    except (TypeError, ValueError):
        return 0

def transform_product(product):
    """
    Transform a single product into a clean, standardized format.
    """

    clean_product = product.copy()

    # -------------------------
    # Text Fields
    # -------------------------

    clean_product["title"] = clean_text(
        product.get("title")
    )

    clean_product["description"] = clean_text(
        product.get("description")
    )

    clean_product["category"] = clean_text(
        product.get("category")
    )

    clean_product["product_type"] = clean_text(
        product.get("product_type")
    )

    # -------------------------
    # Price Fields
    # -------------------------

    clean_product["price"] = clean_price(
        product.get("price")
    )

    clean_product["price_excl_tax"] = clean_price(
        product.get("price_excl_tax")
    )

    clean_product["price_incl_tax"] = clean_price(
        product.get("price_incl_tax")
    )

    clean_product["tax"] = clean_price(
        product.get("tax")
    )

    clean_product["original_price"] = clean_price(
        product.get("original_price")
    )

    # -------------------------
    # Rating
    # -------------------------

    clean_product["rating"] = clean_rating(
        product.get("rating")
    )

    # -------------------------
    # Availability
    # -------------------------

    available, stock_count = clean_availability(
        product.get("availability")
    )

    clean_product["availability"] = available
    clean_product["stock_count"] = stock_count

    # -------------------------
    # Reviews
    # -------------------------

    clean_product["number_of_reviews"] = clean_reviews(
        product.get("number_of_reviews")
    )

    return clean_product

def save_clean_data(products):
    """
    Save cleaned products to JSON.
    """

    with open(CLEAN_FILE, "w", encoding="utf-8") as file:

        json.dump(
            products,
            file,
            indent=4,
            ensure_ascii=False
        )

    logging.info(f"Saved {len(products)} cleaned products.")

def transform_products(products):
    """
    Transform all products.
    """

    clean_products = []

    for product in products:

        try:

            clean_product = transform_product(product)

            clean_products.append(clean_product)

        except Exception as e:

            logging.error(
                f"Failed to transform product: {product.get('title')}"
            )

            logging.error(e)

    logging.info(
        f"Successfully transformed {len(clean_products)} products."
    )

    return clean_products

def main():

    logging.info("Starting Transformation Pipeline...")

    raw_products = load_raw_data()

    clean_products = transform_products(raw_products)

    save_clean_data(clean_products)

    logging.info("=" * 50)
    logging.info(f"Total Products : {len(clean_products)}")
    logging.info(f"Saved File : {CLEAN_FILE}")
    logging.info("Transformation Completed Successfully!")
    logging.info("=" * 50)


if __name__ == "__main__":
    main()
