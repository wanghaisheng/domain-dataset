#!/usr/bin/env python
# MassRDAP - developed by acidvegas (https://git.acid.vegas/massrdap)

import asyncio
import logging
import json
import re
import os, random
from datetime import datetime

import pandas as pd
from DataRecorder import Recorder
import time
import aiohttp_socks

import httpx

# try:
#     import aiofiles
# except ImportError:
#     raise ImportError('missing required aiofiles library (pip install aiofiles)')

try:
    import aiohttp
except ImportError:
    raise ImportError("missing required aiohttp library (pip install aiohttp)")
import asyncio
from contextlib import asynccontextmanager
from dbhelper import DatabaseManager

# Usage
# Now you can use db_manager.add_screenshot(), db_manager.read_screenshot_by_url(), etc.
from loguru import logger
import threading
from DPhelper import DPHelper


# Replace this with your actual test URL
test_url = "http://example.com"


MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10
from queue import PriorityQueue, Queue

work_queue = Queue()


from bs4 import BeautifulSoup
import asyncio
import aiohttp
import time
tld_types = {}
country_cctlds_symbols={}
# Semaphore to control concurrency
semaphore = threading.Semaphore(5)  # Allow up to 5 concurrent tasks

# db_manager = DatabaseManager()
filename='majestic_million'
# filename='toolify.ai-organic-competitors--'
filename='cftopai'
filename='toolify-top500'
# filename='character.ai-organic-competitors--'
# filename='efficient.app-organic-competitors--'
# filename='top-domains-1m'
# filename='artifacts'
# filename='ahref-top'
# filename='builtwith-top'
filename = "./tranco_Z377G"
filename="domain-1year"
filename="domain-2year"

folder_path='.'
inputfilepath=filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# logger.info(domains)
outfilepath=inputfilepath.replace('.csv','-prices.csv')
# outfilepath = "top-domains-1m-price.csv"

outfile = Recorder(folder_path+'/'+outfilepath, cache_size=10)
outcffilepath=inputfilepath.replace('.csv','-prices-cfblock.csv')

outcffile = Recorder(folder_path+'/'+outcffilepath, cache_size=10)

def get_tld_types():
# create a key of tlds and their types using detailed csv
    # tld_types = {}
    with open('tld-list-details.csv','r',encoding='utf8') as f:
        for line in f:
            terms = line.strip().replace('"','').split(',')
            tld_types[terms[0]] = terms[1]
            # print('==',tld_types[terms[0]] )

def get_cctld_symbols():
    country_codes = {}
    country_cctlds = {}

    with open('IP2LOCATION-COUNTRY-INFORMATION.CSV','r',encoding='utf8') as f:
        for line in f:
            terms = line.split(',')
            # print(len(terms),terms)
            if len(terms)>11:
                country_code = terms[0].replace('"','')
                country_name = terms[1].replace('"','')
                cctld = terms[-1].replace('"','').replace('\n','')
                symbol = terms[11].replace('"','').replace('\n','')

                country_codes[country_code] = country_name
                country_cctlds[cctld] = country_name    
                country_cctlds_symbols[cctld]=symbol
def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]
def get_proxy():
    proxy = None
    with aiohttp.ClientSession() as session:
        try:
            with session.get("http://demo.spiderpy.cn/get") as response:
                data = response.json()
                proxy = data["proxy"]
                return proxy
        except:
            pass


def get_proxy_proxypool():
    with aiohttp.ClientSession() as session:

        if proxy is None:
            try:
                with session.get("https://proxypool.scrape.center/random") as response:
                    proxy = response.text()
                    return proxy
            except:
                return None

def get_text(html):
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text()

# Function to extract price data from HTTP response
async def extract_price(html_content, domain):
    try:
        # html_content = await response.text()

        # Extract text content from HTML
        text_content = get_text(html_content)
        tld=get_tld(domain)
        currency_symbol="$"
        if tld_types[tld] == "ccTLD":
            currency_symbol=country_cctlds_symbols(tld)

        # Search for price information in the text content
        if not currency_symbol:
            currency_symbol="$"
        escaped_symbol = re.escape(currency_symbol)
        # Construct regex pattern to match currency_symbol followed by digits and a decimal
        pattern = r'({}[\d,]+\.\d{{2}})'.format(escaped_symbol)
        prices = re.findall(pattern, text_content)

        if prices:
            logger.info(f"Found prices for {domain}: {prices}")

            # Example additional processing or logging
            data = {
                'domain': domain,
                'priceurl': domain.split('/')[-1],
                'prices': prices,
                'raw': text_content.replace('\r', '').replace('\n', '')
            }

            # Logging the extracted data
            logger.info(data)

            # Add data to the recorder (modify as per your Recorder class)
            outfile.add_data(data)

            return True
        else:
            logger.info(f"No prices found for {domain}")
            return False

    except aiohttp.ClientConnectionError:
        logger.error(f"Client connection error while extracting prices for {domain}")

    except Exception as e:
        logger.error(f"Exception occurred while extracting prices for {domain}: {e}")


async def submit_radar_with_cookie(
    browser,
    domain: str,
    url: str,
    valid_proxies: list,
    proxy_url: str,
    outfile: Recorder,
):
    async with semaphore:

        retry_count = 0
        page = browser.new_tab()
        url='https://'+url if 'https' not in url else url
        try:
            # with semaphore:
            cookies=None
            if work_queue.qsize() == 0:
                cookies=get_fresh_cookie(page,domain,url,proxy_url)
            if cookies:


                result = await fetch_data(url, valid_proxies=None, data_format="text",cookies=cookies)

                if result:
                    extract_price(result,domain=domain)
                    if proxy_url and proxy_url not in valid_proxies:
                        valid_proxies.append(proxy_url)
                    return result
        except asyncio.TimeoutError:
            logger.error(
                f"Timeout occurred for domain: {domain} with proxy: {proxy_url}"
            )
        except Exception as e:
            logger.error(f"Error occurred: {e}")





# Function to simulate a task asynchronously
async def fetch_data(url, valid_proxies=None, data_format="json",cookies=None):

    retries = 4
    for attempt in range(1, retries + 1):
        try:
            logger.debug('staaartt to get data')
            proxy_url = None  # Example SOCKS5 proxy URL
            if attempt == 3:
                if valid_proxies:
                    proxy_url = random.choice(valid_proxies)
            elif attempt == 2:
                # proxy_url=await get_proxy_proxypool()
                proxy_url = "socks5://127.0.0.1:1080"  # Example SOCKS5 proxy URL
            elif attempt == 4:
                proxy_url = await get_proxy()
            # proxy_url = "socks5://127.0.0.1:9050"  # Example SOCKS5 proxy URL
            connector = (
                aiohttp_socks.ProxyConnector.from_url(proxy_url)
                if proxy_url and proxy_url.startswith("socks")
                else None
            )
            proxy = proxy_url if proxy_url and "http" in proxy_url else None
            print("===proxy", proxy, url)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, proxy=proxy,cookies=cookies) as response:
                    if response.status == 200:
                        # data = await extract_indedate(response, domain)
                        # print('data',data)
                        print(f"Task {url} completed on attempt {attempt}.")
                        return (
                            await response.json()
                            if data_format == "json"
                            else await response.text()
                        )
                    else:
                        print(
                            f"Task {url} failed on attempt {attempt}. Status code: {response.status}"
                        )
        except aiohttp.ClientConnectionError:
            if attempt < retries:
                print(f"Task {url} failed on attempt {attempt}. Retrying...")
            else:
                print(f"Task {url} failed on all {retries} attempts. Skipping.")
                # outfileerror.add_data([domain])

        except Exception:
            if attempt < retries:
                print(f"Task {url} failed on attempt {attempt}. Retrying...")
            else:
                print(f"Task {url} failed on all {retries} attempts. Skipping.")
                # outfileerror.add_data([domain])
def get_price_dp(browser, domain: str, url: str, proxy_url: str):
    """
    Looks up a domain using the RDAP protocol.

    :param domain: The domain to look up.
    :param proxy_url: The proxy URL to use for the request.
    :param semaphore: The semaphore to use for concurrency limiting.
    """
    with semaphore:

        url='https://'+url if 'https' not in url else url

        query_url = url

        logger.info("use proxy_url:{}", proxy_url)

        logger.info("querying:{}", query_url)
        # page = None
        try:
            headless
        except:
            headless = True

        try:



            # page.set.load_mode.eager()
            page = browser.new_tab()
            page.get(query_url)
            page.wait.load_start()
            # dp.autopass()
            # page.bypass(query_url)

            if page.ele('#turnstile-wrapper'):
                print('detected turnstitle')
                iframe_ele = page.ele('#turnstile-wrapper').child().child().shadow_root.child()
                input_ele=iframe_ele('tag:html').children()[1].sr('t:input')
                input_ele.click()
            if '<title>Please Wait... | Cloudflare</title>' not in page.html:
                print("cf challenge success")



                # data = page.cookies(as_dict=False)
                tld=get_tld(domain)
                logger.info(f'tld:{tld}={ tld_types[tld]}')

                currency_symbol="$"
                if tld_types[tld] in ["gTLD","sTLD","grTLD"]:
                    currency_symbol="$"

                elif tld_types[tld] == "ccTLD":
                    currency_symbol=country_cctlds_symbols[tld]
                logger.info(f'currency_symbol:{currency_symbol}')
                # Search for price information in the text content
                if not currency_symbol:
                    currency_symbol="$"
                escaped_symbol = re.escape(currency_symbol)
                # Construct regex pattern to match currency_symbol followed by digits and a decimal
                pattern = r'({}[\d,]+\.\d{{2}})'.format(escaped_symbol)
                prices = re.findall(pattern, page.ele('tag:html').text)
                logger.info(f'prices:{prices}')

                if prices:
                    logger.info(f"Found prices for {domain}: {prices}")

                    # Example additional processing or logging
                    data = {
                        'domain': domain,
                        'priceurl': domain.split('/')[-1],
                        'prices': prices,
                        'raw': page.ele('tag:html').text.replace('\r', '').replace('\n', '')
                    }

                    # Logging the extracted data
                    logger.info(data)

                    # Add data to the recorder (modify as per your Recorder class)
                    outfile.add_data(data)

                    page.close()

                    return data
            else:
                raise InterruptedError("cf challenge failed")
        except asyncio.TimeoutError as e:

            # page.close()

            raise

        except aiohttp.ClientError as e:

            # page.close()

            raise

        except Exception as e:

            print(f"start a new browser to get fresh :{e}")
            # page.close()


        finally:
            if page:
                page.close()
            # if  browser:
            #     browser.close()
            print("finally")





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

    for p in ['http','socks4','socks5']:
        proxyfile = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies\{p}.txt'


        proxy_dir = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies'
        proxyfile = os.path.join(proxy_dir, f'{p}.txt')
        if os.path.exists(proxyfile):

            tmp = open(proxyfile, "r", encoding="utf8").readlines()
            tmp = list(set(tmp))
            logger.info('p',p,len(tmp))
            raw_proxies+= [f'{p}://'+v.replace("\n", "") for v in tmp if "\n" in v]

    raw_proxies=list(set(raw_proxies))
    logger.info('raw count',len(raw_proxies))
    valid_proxies=[]
    # checktasks=[]
    # for proxy_url in raw_proxies:
    #     task = asyncio.create_task(test_proxy('https://revved.com',proxy_url))
    #     checktasks.append(task)

    # for task in checktasks:
    #     good = await task
    #     if good:
    #         valid_proxies.append(proxy_url)
    valid_proxies=raw_proxies
    logger.info('clean count',len(valid_proxies))
    return valid_proxies    
# Function to run tasks asynchronously with specific concurrency
async def run_async_tasks():
    tasks = []
    df = pd.read_csv(inputfilepath, encoding="ISO-8859-1")
    df = df[df['domain'].str.contains('ai', case=False, na=False)]

    # filtered_df = df[df['indexdate'] != 'unk']
    # filtered_df = df[df['indexdate'].str.contains('month', case=False, na=False)]
    

    # # filtered_df = df[df['indexdate'].str.contains('1 year', case=False, na=False)]

    # domains=filtered_df['domain'].to_list()

    # domains=set(domains)
    # df1year = pd.DataFrame(domains, columns=['domain'])
    # df1year.to_csv('domain-1year.csv')
    dp=DPHelper(browser_path=None,HEADLESS=False,proxy_server='socks5://127.0.0.1:1080')

    domains=df['domain'].to_list()

    domains=set(domains)    
    logger.info(f'load domains：{len(domains)}')
    donedomains=[]
    # domains=['tutorai.me','magicslides.app']
    # try:
    #     db_manager = DatabaseManager()

    #     dbdata=db_manager.read_domain_all()

    #     for i in dbdata:
    #         if i.title is not None:
    #             donedomains.append(i.url)        
    # except Exception as e:
    #     logger.info(f'query error: {e}')
    alldonedomains=[]

    if os.path.exists(outfilepath):
        df=pd.read_csv(outfilepath
                    #    ,encoding="ISO-8859-1"
                       )
        alldonedomains=df['domain'].to_list()
    # else:
        # df=pd.read_csv('top-domains-1m.csv')

        # donedomains=df['domain'].to_list()
    alldonedomains=set(alldonedomains)

    logger.info(f'load alldonedomains:{len(list(alldonedomains))}')
    valid_proxies=getlocalproxies()

    donedomains=[element for element in domains if element  in alldonedomains]
    logger.info(f'load done domains {len(donedomains)}')
    tododomains=list(set([cleandomain(i) for i in domains])-set(donedomains))
    logger.info(f'to be done {len(tododomains)}')


    for domain in tododomains:



        domain=cleandomain(domain)
        for suffix in [''
                    #    ,'premium','price','#price','#pricing','pricing','price-plan','pricing-plan','upgrade','purchase'
                       ]:

            url=domain+suffix

            try:

                task = threading.Thread(target=get_price_dp, args=(dp.driver, domain,url,None))
                tasks.append(task)
                task.start()
            except Exception as e:
                print(f"An error occurred while processing {domain}: {e}")
            if len(tasks) >= 10:
        
                [task.join() for task in tasks]
                tasks=[]
    for task in tasks:
        task.join()

# Example usage: Main coroutine
async def main():
    start_time = time.time()
    get_tld_types()
    get_cctld_symbols()
    await run_async_tasks()
    logger.info(f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds")
# Manually manage the event loop in Jupyter Notebook or other environments
if __name__ == "__main__":
    logger.add(filename+'price-debug.log')

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    outfile.record()
    outcffile.record()

