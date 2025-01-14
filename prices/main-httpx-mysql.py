#!/usr/bin/env python
# check product/service price host on the domain
# author :wanghaisheng

import asyncio
import json
import re
import os
from datetime import datetime

import pandas as pd
import time
import markdownify
import hashlib
import httpx
from dotenv import load_dotenv
from loguru import logger
import argparse
import mysql.connector
load_dotenv()

# Cloudflare D1 Configuration
D1_DATABASE_ID = os.getenv('CLOUDFLARE_D1_DATABASE_ID')
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')

CLOUDFLARE_BASE_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}"

# MySQL Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DB = os.getenv('MYSQL_DB', 'pricing_db')

# Configuration for retries and concurrency:
MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10

# Data for TLD type and currency:
lang_names = {}
tld_types = {}
country_cctlds_symbols = {}
country_symbols = {}


# Semaphore to control concurrency:
semaphore = asyncio.Semaphore(100)  # Allow up to 100 concurrent tasks


# Input/Output File Paths:
filename = "majestic_million"
filename = "cftopai"
filename = "toolify-top500"
filename = "./tranco_Z377G"
filename = "domain-1year"


folder_path = "."
inputfilepath = filename + ".csv"
outfilepath = inputfilepath.replace(".csv", "-prices.csv")
outcffilepath = inputfilepath.replace(".csv", "-prices-cfblock.csv")

# setup log files:
logger.add(filename + "price-debug.log")


def mknewdir(dirname):
    if not os.path.exists(f"{dirname}"):
        nowdir = os.getcwd()
        os.mkdir(nowdir + f"\\{dirname}")

def get_tld_types():
    """Create a key of tlds and their types using detailed csv."""
    with open("tld-list-details.csv", "r", encoding="utf8") as f:
        for line in f:
            terms = line.strip().replace('"', "").split(",")
            tld_types[terms[0]] = terms[1]

def get_cctld_symbols():
    """Create maps of country codes and cctlds to names, symbols and language codes."""
    with open("IP2LOCATION-COUNTRY-INFORMATION.CSV", "r", encoding="utf8") as f:
        for line in f:
            terms = line.split(",")
            if len(terms) > 11:
                country_code = terms[0].replace('"', "")
                country_name = terms[1].replace('"', "")
                cctld = terms[-1].replace('"', "").replace("\n", "")
                symbol = terms[11].replace('"', "").replace("\n", "")
                lang_code = terms[12].replace('"', "").replace("\n", "")
                lang_name = terms[13].replace('"', "").replace("\n", "")

                country_cctlds_symbols[cctld] = symbol
                country_symbols[country_code] = symbol
                lang_names[lang_code] = lang_name

def get_full_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]

def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return parts[-1]


def get_language_name(rawtx):
    """Returns the language name for a block of text."""
    import py3langid as langid

    lang = langid.classify(rawtx)
    lagname = lang_names.get(lang[0].upper(), "English")
    return lagname


def get_country_symbols(rawtx):
    """Returns the currency symbol based on a text."""
    import py3langid as langid

    lang = langid.classify(rawtx)
    currencylabel = country_symbols.get(lang[0].upper(), "$")
    return currencylabel


def get_text(html):
    """Extracts all text from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()

def gettext(html):
    """Extracts the main text content from HTML"""
    import trafilatura
    return trafilatura.extract(html, output_format="txt")

def compute_hash(domain, currency, date):
    """Compute a unique hash for each row."""
    hash_input = f"{domain}-{currency}-{date}"
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

def create_table_if_not_exists(use_d1=True, db_path="pricing_data.sqlite"):
    """Creates the table if not exists, for either a D1 or a SQLite database"""

    if use_d1:
        url = f"{CLOUDFLARE_BASE_URL}/query"
        headers = {
            "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
            "Content-Type": "application/json"
        }
        create_query = """
                CREATE TABLE IF NOT EXISTS domain_pricing_data (
                    id TEXT PRIMARY KEY,
                    domain TEXT,
                    lang TEXT,
                    currency TEXT,
                    links TEXT,
                    prices TEXT,
                    priceplans TEXT,
                    htmlpath TEXT,
                    mdpath TEXT
                );
            """
        try:
            with httpx.Client() as client:
                response = client.post(url, headers=headers, json={"sql": create_query})
                response.raise_for_status()
                print("D1 Table created successfully.")
        except httpx.RequestError as e:
            print(f"Failed to create table domain_pricing_data: {e}")
    else:
       conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        cursor = conn.cursor()
        create_query = """
           CREATE TABLE IF NOT EXISTS domain_pricing_data (
              id VARCHAR(255) PRIMARY KEY,
              domain VARCHAR(255),
              lang VARCHAR(255),
              currency VARCHAR(255),
               links TEXT,
               prices TEXT,
              priceplans TEXT,
              htmlpath TEXT,
              mdpath TEXT
              );
           """
        try:
           cursor.execute(create_query)
           conn.commit()
           print('MySQL Table created.')
        except mysql.connector.Error as e:
            print(f'Error creating MySql table: {e}')
        finally:
            conn.close()


def insert_into_db(data, batch_size=10, use_d1=True, db_path="pricing_data.sqlite"):
    """Inserts data into either a D1 or a local SQLite database"""
    if use_d1:
        insert_into_d1(data, batch_size)
    else:
        insert_into_mysql(data, batch_size)


def insert_into_mysql(data, batch_size,):
    """Insert rows into a MySQL database with hash checks and batch inserts."""
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = conn.cursor()
    if not data:
        print("No data to insert.")
        return
    # Prepare and execute batch inserts
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        values_list = []
        for row in batch:
             hash_id = compute_hash(row['domain'], row['currency'], str(datetime.now()))
            # Ensuring all values are properly formatted as strings for SQL
             values_list.append(f"('{hash_id}', '{row['domain']}', '{row['lang']}', '{row['currency']}', "
               f"'{escape_sql(row['links'])}', '{escape_sql(row['prices'])}', '{escape_sql(row['priceplans'])}', '{escape_sql(row['htmlpath'])}', '{escape_sql(row['mdpath'])}')")
            
        values_str = ", ".join(values_list)

        insert_query = (
            "INSERT IGNORE INTO domain_pricing_data (id, domain, lang, currency, links, prices, priceplans, htmlpath, mdpath) VALUES "
            + values_str + ";"
            )
        try:
            cursor.execute(insert_query)
            conn.commit()
            print(f"Inserted batch {i // batch_size + 1} to MySQL successfully.")

        except mysql.connector.Error as e:
           print(f"Failed to insert batch {i // batch_size + 1}: {e}")
        finally:
             conn.close()
def insert_into_d1(data, batch_size):
    """Insert rows into a D1 database with hash checks and batch inserts."""
    url = f"{CLOUDFLARE_BASE_URL}/query"
    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
        "Content-Type": "application/json"
    }
    if not data:
      print("No data to insert.")
      return
    # Prepare and execute batch inserts
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        values_list = []
        for row in batch:
            hash_id = compute_hash(row['domain'], row['currency'], str(datetime.now()))
            # Ensuring all values are properly formatted as strings for SQL
            values_list.append(f"('{hash_id}', '{row['domain']}', '{row['lang']}', '{row['currency']}', "
                               f"'{escape_sql(row['links'])}', '{escape_sql(row['prices'])}', '{escape_sql(row['priceplans'])}', '{escape_sql(row['htmlpath'])}', '{escape_sql(row['mdpath'])}')")

         # Joining the values for batch insert
        values_str = ", ".join(values_list)
        insert_query = (
            "INSERT OR IGNORE INTO domain_pricing_data (id, domain, lang, currency, links, prices, priceplans, htmlpath, mdpath) VALUES "
            + values_str + ";"
        )

        try:
            with httpx.Client() as client:
                response = client.post(url, headers=headers, json={"sql": insert_query})
                response.raise_for_status()
                print(f"Inserted batch {i // batch_size + 1} to D1 successfully.")
        except httpx.RequestError as e:
            print(f"Failed to insert batch {i // batch_size + 1}: {e}")
            if response:
                print(response.json())
async def extract_price(html_content, domain, use_d1):
    """Extracts price data."""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        htmlpath = 'prices/html/' + domain + '.html'
        mdpath = ''
        prcieplan = ''
        prices = []
        links = []

        if not os.path.exists(htmlpath):
            with open(
                    htmlpath, "w", encoding="utf8"
                ) as f:
                f.write(html_content)

        human_readble_text = gettext(html_content)
        lang = get_language_name(human_readble_text)
        currency_symbol = get_country_symbols(human_readble_text)

        # Search for price information in the text content
        if human_readble_text:
            for line in human_readble_text.split("\n"):
                if currency_symbol in line:
                    prices.append(line)
                    logger.info(f"found price:{line}")
        logger.info(f"prices texts:{prices}")

        # Look for links with price keywords:
        for logger_info in [
            "check price link",
            "check pricing link",
            "check purchase link",
            "check premium link",
            "check upgrade link",
        ]:
            logger.info(logger_info)
            if logger_info == "check price link" and soup.find(
                "a", href=lambda href: href and "price" in href.lower()
            ):
              links.append(
                  soup.find(
                     "a", href=lambda href: href and "price" in href.lower()
                  )["href"]
              )
            elif logger_info == "check pricing link" and soup.find(
                "a", href=lambda href: href and "pricing" in href.lower()
            ):
                links.append(
                  soup.find(
                      "a", href=lambda href: href and "pricing" in href.lower()
                    )["href"]
                )
            elif logger_info == "check purchase link" and soup.find(
                "a", href=lambda href: href and "purchase" in href.lower()
            ):
                 links.append(
                    soup.find(
                        "a", href=lambda href: href and "purchase" in href.lower()
                    )["href"]
                )
            elif logger_info == "check premium link":
                premium_link = soup.find(
                    "a", href=lambda href: href and "premium" in href.lower()
                 )
                if premium_link and "css" not in premium_link["href"]:
                    links.append(premium_link["href"])

            elif logger_info == "check upgrade link" and soup.find(
                "a", href=lambda href: href and "upgrade" in href.lower()
            ):
                links.append(
                    soup.find(
                        "a", href=lambda href: href and "upgrade" in href.lower()
                    )["href"]
                )

        extractmode = 'regex'
        extractmode = 'llm'
        if extractmode == 'regex':
            prcieplan = ''
            try:
                matching_elements = soup.find_all(
                    lambda tag: tag.name in ["section", "div"]
                    and any(
                        text.lower() in ["price", "pricing"]
                        for text in tag.text.lower().split()
                    )
                    and any(
                        text.lower() in ["year", "month"]
                        for text in tag.text.lower().split()
                    )
                    and any(
                        text.lower() in [currency_symbol]
                        for text in tag.text.lower().split()
                    )
                )
                if matching_elements and len(matching_elements)>0:
                    prcieplan=[trafilatura.extract(element, output_format="txt") for element in matching_elements]
            except:
                pass
            logger.info(f"Found prcieplan for {domain}: {prcieplan}")
            raw = None
            logger.info('convert html to md')
            body_elm = soup.find("body")
            webpage_text = ""
            if body_elm:
                webpage_text = markdownify.MarkdownConverter(newline_style='backslash').convert_soup(body_elm)
            else:
                webpage_text = markdownify.MarkdownConverter().convert_soup(soup)
            mdpath='prices/md/'+domain+'.md'
            if not  os.path.exists(mdpath):
                with open(
                    mdpath, "w", encoding="utf8"
                ) as f:
                    f.write(webpage_text)
        data = {
            "domain": domain,
            "lang": lang,
            "currency": currency_symbol,
             "links": json.dumps(links),
             "prices": json.dumps(prices) if len(prices) > 0 else None,
            "priceplans": prcieplan,
            "htmlpath": htmlpath,
            "mdpath": mdpath,
        }
        logger.info(f"add data:{domain}")
        # Add data to the database
        insert_into_db([data], use_d1=use_d1)
        logger.info("saved data to db")
    except Exception as e:
        logger.error(f"Exception occurred while extracting prices for {domain}: {e}")

async def check_url_exists(url):
     try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            return True
     except:
        return False

async def get_priceplan(
    domain: str,
    url: str,
    valid_proxies,
    use_d1=True
):
    async with semaphore:
      url = "https://" + url if "https" not in url else url
      hasPricingEndpoint= False
      for suffix in ['/pricing', '/price-plan']:
        if await check_url_exists(url+suffix):
            hasPricingEndpoint=True
            url=url+suffix
            break
      if hasPricingEndpoint or not suffix:
            try:
                result = await fetch_data(
                    url, data_format="text", cookies=None
                )
                if result:
                    await extract_price(result, domain=domain, use_d1=use_d1)
                    return result
            except asyncio.TimeoutError:
                 logger.error(f"Timeout occurred for domain: {domain}")
            except Exception as e:
                 logger.error(f"Error occurred: {e}")


# Function to simulate a task asynchronously
async def fetch_data(url, data_format="json", cookies=None):
    retries = 1
    for attempt in range(1, retries + 1):
        try:
            logger.debug("staaartt to get data")
            headers={
                    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6533.119 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"

            }
            async with httpx.AsyncClient(headers=headers) as client:
                response = await client.get(url)
                response.raise_for_status()
                if response.status_code == 200:
                    logger.debug(f"Task {url} completed on attempt {attempt}.")
                    return (
                        response.json()
                        if data_format == "json"
                        else response.text
                    )
        except httpx.RequestError as exc:
            if attempt < retries:
                logger.debug(f"Task {url} failed on attempt {attempt}. Retrying...{exc}")
                logger.debug(f"An error occurred while requesting {exc.request.url!r}.")
                # raise exc  # Let the caller handle retries
            else:
                logger.debug(f"Task {url} failed on all {retries} attempts. Skipping {exc}.")
                logger.debug(f"An error occurred while requesting {exc.request.url!r}.")
        except httpx.HTTPStatusError as exc:
            if attempt < retries:
                logger.debug(f"Task {url} failed on attempt {attempt}. Retrying...{exc}")
                logger.debug(
                    f"Error response {exc.response.status_code} while requesting {exc.request.url!r}."
                )
                # raise exc  # Let the caller handle retries
            else:
                 logger.debug(f"Task {url} failed on all {retries} attempts. Skipping.{exc}")
                 logger.debug(
                     f"Error response {exc.response.status_code} while requesting {exc.request.url!r}."
                )
        except Exception as e:
           if attempt < retries:
                logger.error(f"Task {url} failed on attempt {attempt}. Retrying...{e}")
           else:
                logger.error(f"Task {url} failed on all {retries} attempts. Skipping.{e}")


def cleandomain(domain):
    if isinstance(domain, str) == False:
        domain = str(domain)
    domain = domain.strip()
    if "https://" in domain:
        domain = domain.replace("https://", "")
    if "http://" in domain:
        domain = domain.replace("http://", "")
    if "www." in domain:
        domain = domain.replace("www.", "")
    if domain.endswith("/"):
        domain = domain.rstrip("/")
    return domain

async def run_async_tasks(use_d1=True, db_path="pricing_data.sqlite"):
    tasks = []
    df = pd.read_csv(inputfilepath, encoding="ISO-8859-1")
    domains = df["domain"].to_list()
    domains = set(domains)
    domains = [cleandomain(element) for element in domains]

    logger.info(f"load domains：{len(domains)}")
    donedomains = []
    alldonedomains = []

    if os.path.exists(outfilepath):
        df = pd.read_csv(outfilepath)
        alldonedomains = df["domain"].to_list()
    alldonedomains = set(alldonedomains)
    logger.info(f"load alldonedomains:{len(list(alldonedomains))}")

    donedomains = [element for element in domains if element in alldonedomains]
    logger.info(f"load done domains {len(donedomains)}")
    tododomains = list(set([cleandomain(i) for i in domains]) - set(donedomains))
    logger.info(f"to be done {len(tododomains)}")
    import time
    time.sleep(60)
    counts=len(tododomains)
    try:
        counts = os.getenv("counts")
        counts=int(counts)
    except:
        pass
    for domain in tododomains:
         domain = cleandomain(domain)
         task = asyncio.create_task(
            get_priceplan(domain, domain , None, use_d1=use_d1)
         )
         tasks.append(task)
         if len(tasks) >= 100:
              await asyncio.gather(*tasks)
              tasks = []
    await asyncio.gather(*tasks)


async def main():
    start_time = time.time()
    get_tld_types()
    get_cctld_symbols()
    mknewdir('prices')
    mknewdir('prices/html')
    mknewdir('prices/md')

    parser = argparse.ArgumentParser(description="Process domains and save prices.")
    parser.add_argument(
        "--dbtype", type=str, choices=["d1", "mysql", "sqlite"], default="d1",
        help="Database type to use: 'd1' for Cloudflare D1 or 'sqlite' for local SQLite, or mysql"
    )
    parser.add_argument(
        "--dbpath", type=str, default="pricing_data.sqlite",
        help="Path to the SQLite database file (only for SQLite)"
    )
    args = parser.parse_args()

    use_d1 = args.dbtype == "d1"
    use_mysql = args.dbtype =="mysql"
    db_path = args.dbpath if not use_d1 else None

    create_table_if_not_exists(use_d1=use_d1, db_path=db_path)

    await run_async_tasks(use_d1=use_d1, db_path=db_path)
    logger.info(
        f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds"
    )

# Manually manage the event loop in Jupyter Notebook or other environments
if __name__ == "__main__":
    logger.add(filename + "price-debug.log")
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
