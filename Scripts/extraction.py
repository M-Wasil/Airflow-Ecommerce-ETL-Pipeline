from pathlib import Path
import json
import logging
import random
import time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================================
# Project Paths
# ==========================================================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

RAW_FILE = DATA_DIR / "raw_products.json"

# ==========================================================
# Configuration
# ==========================================================

BASE_URL = "https://books.toscrape.com/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    )
}

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/138.0 Safari/537.36"
    )
})

session.verify = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==========================================================
# Helper Functions
# ==========================================================

def fetch_page(url, retries=3):
    """
    Fetch a webpage and return a BeautifulSoup object.
    Retries before giving up.
    """

    for attempt in range(1, retries + 1):

        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            response.encoding = "utf-8"
            return BeautifulSoup(response.text, "html.parser")

        except requests.exceptions.RequestException as e:

            logging.warning(
                f"Attempt {attempt}/{retries} failed for {url}"
            )

            logging.warning(e)

            if attempt < retries:
                time.sleep(2)

    logging.error(f"Skipping page: {url}")

    return None

def extract_product_details(product_url):
    """
    Extract additional information from an individual product page.
    """

    soup = fetch_page(product_url)

    if soup is None:
        return {}

    product_info = {}

    # ---------------------------
    # Description
    # ---------------------------

    description = soup.find("div", id="product_description")

    if description:
        description = description.find_next("p").get_text(strip=True)
    else:
        description = None

    product_info["description"] = description

    # ---------------------------
    # Availability
    # ---------------------------

    availability = soup.find("p", class_="instock availability")

    if availability:
        availability = availability.get_text(strip=True)

    product_info["availability"] = availability

    # ---------------------------
    # Category
    # ---------------------------

    breadcrumbs = soup.select("ul.breadcrumb li a")

    if len(breadcrumbs) >= 3:
        category = breadcrumbs[2].get_text(strip=True)
    else:
        category = None

    product_info["category"] = category

    # ---------------------------
    # Product Information Table
    # ---------------------------

    field_mapping = {
        "UPC": "upc",
        "Product Type": "product_type",
        "Price (excl. tax)": "price_excl_tax",
        "Price (incl. tax)": "price_incl_tax",
        "Tax": "tax",
        "Availability": "availability_table",
        "Number of reviews": "number_of_reviews"
    }

    table = soup.find("table", class_="table table-striped")

    if table:

        rows = table.find_all("tr")

        for row in rows:

            key = row.th.get_text(strip=True)
            if key == "Availability":
                continue
            value = row.td.get_text(strip=True)

            normalized_key = field_mapping.get(
                key,
                key.lower().replace(" ", "_")
            )

            product_info[normalized_key] = value
    time.sleep(random.uniform(0.3, 0.7))
    return product_info

def extract_products(soup,current_url):
    """
    Extract products from a single catalogue page.
    """

    products = []

    books = soup.find_all("article", class_="product_pod")

    logging.info(f"Found {len(books)} books on current page.")

    for book in books:

        title = book.h3.a["title"]

        price = book.find("p", class_="price_color").text

        rating = book.find("p", class_="star-rating")["class"][-1]

        image_url = urljoin(current_url, book.img["src"])

        product_url = urljoin(current_url, book.h3.a["href"])

        product = {
            "title": title,
            "description": None,
            "price": price,
            "original_price": None,
            "image_url": image_url,
            "availability": None,
            "rating": rating,
            "variants": None,
            "product_url": product_url
        }
        
        details = extract_product_details(product_url)

        product.update(details)
        products.append(product)

    return products


def get_next_page(soup, current_url):
    """
    Return the next catalogue page URL.
    """

    next_button = soup.find("li", class_="next")

    if next_button is None:
        return None

    return urljoin(current_url, next_button.a["href"])


def extract_all_products():
    """
    Scrape every catalogue page.
    """

    all_products = []

    current_url = BASE_URL

    page = 1

    while current_url:

        logging.info(f"Scraping Page {page}")

        soup = fetch_page(current_url)

        if soup is None:
            logging.error(f"Failed to scrape Page {page}")
            break

        products = extract_products(soup,current_url)

        all_products.extend(products)

        current_url = get_next_page(soup, current_url)

        page += 1

        delay = random.uniform(1, 2)

        logging.info(f"Waiting {delay:.2f} seconds...")

        time.sleep(delay)

    return all_products


def save_raw_data(data):
    """
    Save scraped data as JSON.
    """

    with open(RAW_FILE, "w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=4,
            ensure_ascii=False
        )


# ==========================================================
# Main
# ==========================================================

def main():

    logging.info("Starting Extraction Pipeline...")

    products = extract_all_products()
    if not products:
        raise RuntimeError("Extraction failed. No products were scraped.")

    save_raw_data(products)

    logging.info("=" * 50)
    logging.info(f"Total Products Scraped : {len(products)}")
    logging.info(f"Saved File : {RAW_FILE}")
    logging.info("Extraction Completed Successfully!")
    logging.info("=" * 50)


if __name__ == "__main__":
    main()
