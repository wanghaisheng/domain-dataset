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
import markdownify

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
semaphore = asyncio.Semaphore(100)  # Allow up to 5 concurrent tasks

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
# filename = "./tranco_Z377G"
# filename = "domain-1year"
# filename = "domain-2year"
# filename = "domain-ai"
# filename='top1000ai'
# filename='.reports/character.ai-organic-competitors--.csv'
folder_path = "."
inputfilepath = filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# logger.info(domains)
outfilepath = inputfilepath.replace(".csv", "-rankhistory.csv")
# outfilepath = "domain-ai-price.csv"

outfile = Recorder(folder_path + "/" + outfilepath, cache_size=10)
outcffilepath = inputfilepath.replace(".csv", "-rankhistory-cfblock.csv")

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


def get_full_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]

def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return parts[-1]


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
    currencylabel = country_symbols.get(lang[0].upper(), "$")
    return currencylabel


def get_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()


def gettext(html):
    # https://github.com/wanghaisheng/htmltotext-benchmark/blob/master/algorithms.py
    import trafilatura

    return trafilatura.extract(html, output_format="txt")


# Function to extract price data from HTTP response
async def extract_rank(content, domain):
    """
    {
    "ranks": [
        {
            "date": "2024-08-06",
            "rank": 121188
        },
        {
            "date": "2024-08-05",
            "rank": 121080
        },
        {
            "date": "2024-08-04",
            "rank": 121173
        },
        {
            "date": "2024-08-03",
            "rank": 121195
        },
        {
            "date": "2024-08-02",
            "rank": 121306
        },
        {
            "date": "2024-08-01",
            "rank": 121373
        },
        {
            "date": "2024-07-31",
            "rank": 121364
        },
        {
            "date": "2024-07-30",
            "rank": 121507
        },
        {
            "date": "2024-07-29",
            "rank": 120691
        },
        {
            "date": "2024-07-28",
            "rank": 120149
        },
        {
            "date": "2024-07-27",
            "rank": 120006
        },
        {
            "date": "2024-07-26",
            "rank": 120042
        },
        {
            "date": "2024-07-25",
            "rank": 120163
        },
        {
            "date": "2024-07-24",
            "rank": 120112
        },
        {
            "date": "2024-07-23",
            "rank": 120181
        },
        {
            "date": "2024-07-22",
            "rank": 120420
        },
        {
            "date": "2024-07-21",
            "rank": 121313
        },
        {
            "date": "2024-07-20",
            "rank": 120992
        },
        {
            "date": "2024-07-19",
            "rank": 120979
        },
        {
            "date": "2024-07-18",
            "rank": 121145
        },
        {
            "date": "2024-07-17",
            "rank": 121724
        },
        {
            "date": "2024-07-16",
            "rank": 123167
        },
        {
            "date": "2024-07-15",
            "rank": 123164
        },
        {
            "date": "2024-07-14",
            "rank": 123381
        },
        {
            "date": "2024-07-13",
            "rank": 123360
        },
        {
            "date": "2024-07-12",
            "rank": 123267
        },
        {
            "date": "2024-07-11",
            "rank": 123198
        },
        {
            "date": "2024-07-10",
            "rank": 123191
        },
        {
            "date": "2024-07-09",
            "rank": 123126
        },
        {
            "date": "2024-07-08",
            "rank": 122549
        },
        {
            "date": "2024-07-07",
            "rank": 122271
        },
        {
            "date": "2024-07-06",
            "rank": 121910
        },
        {
            "date": "2024-07-05",
            "rank": 121512
        },
        {
            "date": "2024-07-04",
            "rank": 121362
        },
        {
            "date": "2024-07-03",
            "rank": 120920
        },
        {
            "date": "2024-07-02",
            "rank": 119917
        },
        {
            "date": "2024-07-01",
            "rank": 118648
        }
    ],
    "domain": "toolify.ai"
}"""


    try:
        logger.info(f"add data:{domain}")
        rankdata=content.get('ranks')
        if not rankdata:
            logger.debug(f'there is no json data in there:{domain}')
            return 
        for i in rankdata:
            date=i.get('date','')
            rank=i.get('rank','')

            data = {
                "domain": domain,
                "date": date,
                "rank": rank
            }

        # Logging the extracted data
        # logger.info(data)

        # Add data to the recorder (modify as per your Recorder class)
            outfile.add_data(data)
        logger.info("save data")

    except Exception as e:
        logger.error(f"Exception occurred while extracting prices for {domain}: {e}")


async def get_30d_rank(
    domain: str,
    url: str,
    valid_proxies: list,
):
    async with semaphore:
        domain=cleandomain(domain)

        url="https://tranco-list.eu/api/ranks/domain/{domain}"

        try:
            # with semaphore:
            result = await fetch_data(
                url, valid_proxies=valid_proxies, data_format="json", cookies=None
            )

            if result:
                await extract_rank(result, domain=domain)

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

                    # break
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
            outcffile.add_data({"domain":url,'status':exc.response.status_code})

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
                get_30d_rank(domain, domain + "/" + suffix, valid_proxies)
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
    logger.add(filename + "rank-history-debug.log")

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    outfile.record()
    outcffile.record()
