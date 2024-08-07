#!/usr/bin/env python
# check product/service price host on the domain
# author :wanghaisheng

import asyncio
import json
import re
import os, random
from datetime import datetime

import pandas as pd
from DataRecorder import Recorder
import time

import httpx
from dbhelper import DatabaseManager

# Usage
# Now you can use db_manager.add_screenshot(), db_manager.read_screenshot_by_url(), etc.
from loguru import logger

from bs4 import BeautifulSoup


# Replace this with your actual test URL
test_url = "http://example.com"


MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10

lang_names = {}
tld_types = {}
country_cctlds_symbols = {}
country_symbols = {}
# Semaphore to control concurrency
semaphore = asyncio.Semaphore(5)  # Allow up to 5 concurrent tasks

# db_manager = DatabaseManager()
filename = "majestic_million"
# filename='toolify.ai-organic-competitors--'
filename = "cftopai"
filename = "toolify-top500"
# filename='character.ai-organic-competitors--'
# filename='efficient.app-organic-competitors--'
# filename='top-domains-1m'
# filename='artifacts'
# filename='ahref-top'
# filename='builtwith-top'
filename = "./tranco_Z377G"
filename = "domain-1year"
# filename = "domain-2year"
# filename = "domain-ai"
# filename='top1000ai'
# filename='.reports/character.ai-organic-competitors--.csv'
folder_path = "."
inputfilepath = filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# logger.info(domains)
outfilepath = inputfilepath.replace(".csv", "-prices.csv")
# outfilepath = "domain-ai-price.csv"

outfile = Recorder(folder_path + "/" + outfilepath, cache_size=10)
outcffilepath = inputfilepath.replace(".csv", "-prices-cfblock.csv")

outcffile = Recorder(folder_path + "/" + outcffilepath, cache_size=10)


def get_tld_types():
    # create a key of tlds and their types using detailed csv
    # tld_types = {}
    with open("tld-list-details.csv", "r", encoding="utf8") as f:
        for line in f:
            terms = line.strip().replace('"', "").split(",")
            tld_types[terms[0]] = terms[1]
            # logger.debug('==',tld_types[terms[0]] )


def get_cctld_symbols():
    country_codes = {}
    country_cctlds = {}

    with open("IP2LOCATION-COUNTRY-INFORMATION.CSV", "r", encoding="utf8") as f:
        for line in f:
            terms = line.split(",")
            # logger.debug(len(terms),terms)
            if len(terms) > 11:
                country_code = terms[0].replace('"', "")
                country_name = terms[1].replace('"', "")
                cctld = terms[-1].replace('"', "").replace("\n", "")
                symbol = terms[11].replace('"', "").replace("\n", "")
                lang_code = terms[12].replace('"', "").replace("\n", "")
                lang_name = terms[13].replace('"', "").replace("\n", "")

                country_codes[country_code] = country_name
                country_cctlds[cctld] = country_name
                country_cctlds_symbols[cctld] = symbol
                country_symbols[country_code] = symbol
                lang_names[lang_code] = lang_name


def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]


async def get_proxy():
    query_url = "http://demo.spiderpy.cn/get"
    async with httpx.AsyncClient() as client:
        response = await client.get(query_url)
        try:
            proxy = response.json()
            return proxy
        except:
            return None


async def get_proxy_proxypool():
    query_url = "https://proxypool.scrape.center/random"
    async with httpx.AsyncClient() as client:
        response = await client.get(query_url)
        try:
            proxy = response.text
            return proxy
        except:
            return None


# Example usage
# language_code = 'fr'
# logger.debug(f"The language code '{language_code}' is for {get_language_name(language_code)}.")s
def get_language_name(rawtx):
    import py3langid as langid

    lang = langid.classify(rawtx)
    # logger.debug('========',lang)
    lagname = lang_names.get(lang[0].upper(), "English")
    return lagname


def get_country_symbols(rawtx):
    import py3langid as langid

    lang = langid.classify(rawtx)
    currencylabel = country_symbols.get(lang, "$")
    return currencylabel


def get_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()


def gettext(html):
    # https://github.com/wanghaisheng/htmltotext-benchmark/blob/master/algorithms.py
    import trafilatura

    return trafilatura.extract(html, output_format="txt")


# Function to extract price data from HTTP response
async def extract_price(html_content, domain):
    try:
        # html_content = await response.text()
        # Extract text content from HTML
        soup = BeautifulSoup(html_content, "html.parser")

        human_readble_text = gettext(html_content)
        lang = get_language_name(human_readble_text)
        currency_symbol = get_country_symbols(human_readble_text)

        prices = []
        logger.info("check price text")

        # data = page.cookies(as_dict=False)
        tld = get_tld(domain)
        logger.info(f"tld:{tld}={ tld_types[tld]}")

        if not currency_symbol:
            if tld_types[tld] in ["gTLD", "sTLD", "grTLD"]:
                currency_symbol = "$"

            elif tld_types[tld] == "ccTLD":
                currency_symbol = country_cctlds_symbols[tld]
        logger.info(f"currency_symbol:{currency_symbol}")
        # Search for price information in the text content
        if human_readble_text:
            for line in human_readble_text.split("\n"):
                if currency_symbol in line:
                    prices.append(line)
                    logger.info(f"found price:{line}")

        # pricethere='price' in page.html or 'pricing' in page.html
        logger.info(f"prices texts:{prices}")

        logger.info(f"Found prices for {domain}: {prices}")
        prcieplan = None
        try:

            # Find sections or divs where any children element text contains 'price' or 'pricing'
            matching_elements = soup.find_all(
                lambda tag: tag.name in ["section", "div"]
                and any(
                    text.lower() in ["price", "pricing"]
                    for text in tag.text.lower().split()
                )
            )

            # Collect all text content from matching elements
            prcieplan = [element.get_text(strip=True) for element in matching_elements]
        except:
            pass
        logger.info(f"Found prcieplan for {domain}: {prcieplan}")
        links = []
        if len(prices) == 0 and len(prcieplan) == 0:

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

        logger.info(f"add data:{domain}")
        raw = None

        #  tag:body 会得到一个chromeFrame的类  没有text 会报错

        data = {
            "domain": domain,
            "lang": lang,
            "currency": currency_symbol,
            # 'priceurl': domain.split('/')[-1],
            "links": links,
            "prices": prices,
            "price-plans": prcieplan,
            "raw": json.dumps(human_readble_text.replace("\r", " ").replace("\n", " ")),
        }

        # Logging the extracted data
        # logger.info(data)

        # Add data to the recorder (modify as per your Recorder class)
        outfile.add_data(data)
        logger.info("save data")

    except Exception as e:
        logger.error(f"Exception occurred while extracting prices for {domain}: {e}")


async def get_priceplan(
    domain: str,
    url: str,
    valid_proxies: list,
):
    async with semaphore:
        url = "https://" + url if "https" not in url else url
        try:
            # with semaphore:
            result = await fetch_data(
                url, valid_proxies=valid_proxies, data_format="text", cookies=None
            )

            if result:
                await extract_price(result, domain=domain)

                return result
        except asyncio.TimeoutError:
            logger.error(f"Timeout occurred for domain: {domain}")
        except Exception as e:
            logger.error(f"Error occurred: {e}")


# Function to simulate a task asynchronously
async def fetch_data(url, valid_proxies=None, data_format="json", cookies=None):

    retries = 4
    for attempt in range(1, retries + 1):
        try:
            logger.debug("staaartt to get data")
            proxy_url = None  # Example SOCKS5 proxy URL
            proxy_url = "socks5://127.0.0.1:1080"  # Example SOCKS5 proxy URL

            if attempt == 3:
                if valid_proxies:
                    proxy_url = random.choice(valid_proxies)
            elif attempt == 2:
                # proxy_url=await get_proxy_proxypool()
                proxy_url = "socks5://127.0.0.1:1080"  # Example SOCKS5 proxy URL
            elif attempt == 4:
                proxy_url = await get_proxy()
            # proxy_url = "socks5://127.0.0.1:9050"  # Example SOCKS5 proxy URL
            # pip install httpx[socks]
            async with httpx.AsyncClient(proxy=proxy_url) as client:
                response = await client.get(url)
                response.raise_for_status()
                if response.status_code == 200:
                    # data = await extract_indedate(response, domain)
                    # logger.debug('data',data)
                    logger.debug(f"Task {url} completed on attempt {attempt}.")
                    return (
                         response.json()
                        if data_format == "json"
                        else  response.text
                    )
                else:
                    logger.debug(
                        f"Task {url} failed on attempt {attempt}. Status code: {response.status_code}"
                    )
                    outcffile.add_data({"domain":url,'status':response.status_code})
                    break
        except httpx.RequestError as exc:
            if attempt < retries:
                logger.debug(f"Task {url} failed on attempt {attempt}. Retrying...{exc}")
                logger.debug(f"An error occurred while requesting {exc.request.url!r}.")

                # raise exc  # Let the caller handle retries

            else:
                logger.debug(f"Task {url} failed on all {retries} attempts. Skipping {exc}.")
                logger.debug(f"An error occurred while requesting {exc.request.url!r}.")

                # outfileerror.add_data([domain])
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
                logger.debug(f"Task {url} failed on attempt {attempt}. Retrying...{e}")

            else:
                logger.debug(f"Task {url} failed on all {retries} attempts. Skipping.{e}")
                # outfileerror.add_data([domain])


# To run the async function, you would do the following in your main code or script:
# asyncio.run(test_proxy('your_proxy_url_here'))
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


def getlocalproxies():

    raw_proxies = []

    for p in ["http", "socks4", "socks5"]:
        proxyfile = r"D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies\{p}.txt"

        proxy_dir = r"D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies"
        proxyfile = os.path.join(proxy_dir, f"{p}.txt")
        if os.path.exists(proxyfile):

            tmp = open(proxyfile, "r", encoding="utf8").readlines()
            tmp = list(set(tmp))
            logger.info("p", p, len(tmp))
            raw_proxies += [f"{p}://" + v.replace("\n", "") for v in tmp if "\n" in v]

    raw_proxies = list(set(raw_proxies))
    logger.info("raw count", len(raw_proxies))
    valid_proxies = []
    # checktasks=[]
    # for proxy_url in raw_proxies:
    #     task = asyncio.create_task(test_proxy('https://revved.com',proxy_url))
    #     checktasks.append(task)

    # for task in checktasks:
    #     good = await task
    #     if good:
    #         valid_proxies.append(proxy_url)
    valid_proxies = raw_proxies
    logger.info("clean count", len(valid_proxies))
    return valid_proxies


# Function to run tasks asynchronously with specific concurrency
async def run_async_tasks():
    tasks = []
    df = pd.read_csv(inputfilepath, encoding="ISO-8859-1")

    domains = df["domain"].to_list()

    domains = set(domains)
    logger.info(f"load domains：{len(domains)}")
    donedomains = []
    # domains=['tutorai.me','magicslides.app']
    # try:
    #     db_manager = DatabaseManager()

    #     dbdata=db_manager.read_domain_all()

    #     for i in dbdata:
    #         if i.title is not None:
    #             donedomains.append(i.url)
    # except Exception as e:
    #     logger.info(f'query error: {e}')
    alldonedomains = []
    outfilepath = "domain-ai-price.csv"

    if os.path.exists(outfilepath):
        df = pd.read_csv(
            outfilepath
            #    ,encoding="ISO-8859-1"
        )
        alldonedomains = df["domain"].to_list()
    # else:
    # df=pd.read_csv('top-domains-1m.csv')

    # donedomains=df['domain'].to_list()
    alldonedomains = set(alldonedomains)

    logger.info(f"load alldonedomains:{len(list(alldonedomains))}")
    valid_proxies = getlocalproxies()

    donedomains = [element for element in domains if element in alldonedomains]
    logger.info(f"load done domains {len(donedomains)}")
    tododomains = list(set([cleandomain(i) for i in domains]) - set(donedomains))
    logger.info(f"to be done {len(tododomains)}")

    for domain in tododomains:

        domain = cleandomain(domain)

        # if not ".ai" in domain:
        #     continue
        # logger.debug(domain.split(".")[0])
        # if not domain.split(".")[0].endswith("ai"):
        #     continue
        # if not  domain.split('.')[0].startswith("ai"):
        #     continue

        # logger.debug(domain)

        for suffix in [
            ""
            #    ,'premium','price','#price','#pricing','pricing','price-plan','pricing-plan','upgrade','purchase'
        ]:

            url = domain + suffix
            logger.debug(f"add domain:{domain}")

            task = asyncio.create_task(
                get_priceplan(domain, domain + "/" + suffix, valid_proxies)
            )
            tasks.append(task)
            if len(tasks) >= 100:
                # Wait for the current batch of tasks to complete
                await asyncio.gather(*tasks)
                tasks = []
    await asyncio.gather(*tasks)


# Example usage: Main coroutine
async def main():
    start_time = time.time()
    get_tld_types()
    get_cctld_symbols()
    await run_async_tasks()
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
    outfile.record()
    outcffile.record()
