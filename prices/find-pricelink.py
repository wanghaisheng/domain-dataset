import httpx
import xml.etree.ElementTree as ET
import asyncio

async def fetch_robots_txt(client, domain):
    """Fetch the robots.txt file for a domain."""
    url = f"http://{domain}/robots.txt"
    try:
        response = await client.get(url)
        if response.status_code == 200:
            return response.text
        else:
            print(f"[{domain}] Failed to fetch robots.txt. Status code: {response.status_code}")
            return None
    except httpx.RequestError as e:
        print(f"[{domain}] An error occurred while fetching robots.txt: {str(e)}")
        return None

def parse_sitemap_urls(robots_txt):
    """Extract sitemap URLs from the robots.txt content."""
    sitemap_urls = []
    for line in robots_txt.splitlines():
        if line.lower().startswith('sitemap:'):
            sitemap_url = line.split(':', 1)[1].strip()
            sitemap_urls.append(sitemap_url)
    return sitemap_urls

async def fetch_and_parse_sitemap(client, sitemap_url):
    """Fetch and parse a sitemap to find URLs containing 'price'."""
    found_price_urls = []
    try:
        response = await client.get(sitemap_url)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
                loc = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
                if loc is not None and 'price' in loc.text:
                    print(f"Found 'price' in URL: {loc.text}")
                    found_price_urls.append(loc.text)
        else:
            print(f"Failed to fetch sitemap {sitemap_url}. Status code: {response.status_code}")
    except httpx.RequestError as e:
        print(f"An error occurred while fetching sitemap {sitemap_url}: {str(e)}")
    except ET.ParseError as e:
        print(f"Failed to parse sitemap {sitemap_url}: {str(e)}")
    return found_price_urls

async def check_price_urls(client, domain):
    """Check domain/price and domain/price-plan URLs for 404 status."""
    urls_to_check = [f"http://{domain}/price", f"http://{domain}/price-plan"]
    for url in urls_to_check:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                print(f"URL not found (404): {url}")
            else:
                print(f"URL found: {url} (Status: {response.status_code})")
        except httpx.RequestError as e:
            print(f"An error occurred while checking URL {url}: {str(e)}")

async def main(domains):
    async with httpx.AsyncClient() as client:
        for domain in domains:
            print(f"Checking domain: {domain}")
            robots_txt = await fetch_robots_txt(client, domain)
            if robots_txt:
                sitemap_urls = parse_sitemap_urls(robots_txt)
                if sitemap_urls:
                    print(f"Found sitemaps for {domain}: {sitemap_urls}")
                    all_price_urls = []
                    for sitemap_url in sitemap_urls:
                        price_urls = await fetch_and_parse_sitemap(client, sitemap_url)
                        all_price_urls.extend(price_urls)
                    if not all_price_urls:
                        print(f"No 'price' URLs found in sitemaps for {domain}. Checking specific URLs.")
                        await check_price_urls(client, domain)
                else:
                    print(f"No sitemaps found in robots.txt for {domain}")
                    await check_price_urls(client, domain)
            else:
                print(f"Could not retrieve robots.txt for {domain}")
                await check_price_urls(client, domain)

# Example usage with multiple domains
domains_to_check = ['example.com', 'example.org', 'example.net']
asyncio.run(main(domains_to_check))
