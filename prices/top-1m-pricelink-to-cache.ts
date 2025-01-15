import fs from 'fs';
import path from 'path';
import process from 'node:process';
import * as unzipper from 'unzipper';
import csvParser from 'csv-parser';
import httpx from 'httpx';
import Database from 'better-sqlite3';
import * as xml2js from 'xml2js';

// Constants for downloading and processing
const ZIP_FILE_URL = 'https://tranco-list.eu/top-1m.csv.zip';
const ZIP_FILE_PATH = 'top-1m.csv.zip';
const EXTRACTED_CSV_PATH = 'top-1m.csv'; // This will be used after unzipping the file

// Configuration options
const MAX_CONCURRENT_REQUESTS = 100; 
const RATE_LIMIT_DELAY = 1000; 

const filePath = process.argv[2];
if (!filePath) {
  console.error(`File path missing. Usage:

  $ pnpm tsx scripts/updateSqliteDatabaseRows.ts data/persisted-to-cache/database.db
`);
  process.exit(1);
}

// Database connection
const db = new Database(filePath);
db.pragma('journal_mode = WAL');

// Create table if not exists
db.prepare(`
  CREATE TABLE IF NOT EXISTS domain_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    found_price_urls TEXT, 
    checked_urls TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
`).run();

// Function to download and unzip the file
const downloadAndUnzip = async () => {
  const wget = require('child_process').execSync;
  console.log(`Downloading file from ${ZIP_FILE_URL}...`);
  wget(`wget ${ZIP_FILE_URL}`);
  console.log(`Unzipping the downloaded file...`);
  fs.createReadStream(ZIP_FILE_PATH)
    .pipe(unzipper.Extract({ path: '.' }))
    .on('close', () => console.log('File unzipped successfully.'));
};

// Function to parse CSV and extract domains
const parseDomainsFromCSV = async (csvFilePath: string): Promise<string[]> => {
  return new Promise((resolve, reject) => {
    const domains: string[] = [];
    fs.createReadStream(csvFilePath)
      .pipe(csvParser())
      .on('data', (row) => {
        // The domain is in the second column (index 1)
        domains.push(row[Object.keys(row)[1]]);
      })
      .on('end', () => {
        console.log(`Parsed ${domains.length} domains.`);
        resolve(domains);
      })
      .on('error', reject);
  });
};

// Function to fetch robots.txt
const fetchRobotsTxt = async (domain: string): Promise<string | null> => {
  const url = `http://${domain}/robots.txt`;
  try {
    const response = await httpx.get(url);
    if (response.status === 200) {
      return response.data;
    }
    return null;
  } catch (error) {
    console.error(`Error fetching robots.txt for ${domain}: ${error}`);
    return null;
  }
};

// Function to parse the sitemap URLs
const parseSitemapUrls = (robotsTxt: string): string[] => {
  const sitemapUrls: string[] = [];
  const lines = robotsTxt.split('\n');
  for (let line of lines) {
    if (line.toLowerCase().startsWith('sitemap:')) {
      const sitemapUrl = line.split(':', 2)[1].trim();
      sitemapUrls.push(sitemapUrl);
    }
  }
  return sitemapUrls;
};

// Function to fetch and parse the sitemap for price URLs
const fetchAndParseSitemap = async (sitemapUrl: string): Promise<string[]> => {
  const foundPriceUrls: string[] = [];
  try {
    const response = await httpx.get(sitemapUrl);
    if (response.status === 200) {
      const parser = new xml2js.Parser();
      parser.parseString(response.data, (err, result) => {
        if (err) {
          console.error(`Error parsing sitemap ${sitemapUrl}: ${err}`);
          return;
        }
        const urls = result.urlset.url || [];
        for (const url of urls) {
          const loc = url.loc && url.loc[0];
          if (loc && loc.includes('price')) {
            foundPriceUrls.push(loc);
          }
        }
      });
    }
  } catch (error) {
    console.error(`Error fetching sitemap ${sitemapUrl}: ${error}`);
  }
  return foundPriceUrls;
};

// Function to check fallback URLs if no price URLs are found
const checkFallbackUrls = async (domain: string): Promise<string[]> => {
  const urlsToCheck = [`http://${domain}/price`, `http://${domain}/price-plan`];
  const checkedUrls: string[] = [];
  for (let url of urlsToCheck) {
    try {
      const response = await httpx.get(url);
      if (response.status === 404) {
        checkedUrls.push(`404 Not Found: ${url}`);
      } else {
        checkedUrls.push(`Found: ${url} (Status: ${response.status})`);
      }
    } catch (error) {
      checkedUrls.push(`Error: ${url} - ${error}`);
    }
  }
  return checkedUrls;
};

// Function to save results in batches
const saveDomainResults = (domain: string, foundPriceUrls: string[], checkedUrls: string[]): void => {
  const insert = db.prepare(`
    INSERT INTO domain_results (domain, found_price_urls, checked_urls)
    VALUES (@domain, @found_price_urls, @checked_urls)
  `);

  insert.run({
    domain,
    found_price_urls: JSON.stringify(foundPriceUrls),
    checked_urls: JSON.stringify(checkedUrls)
  });
};

// Function to process domains asynchronously with concurrency control
const processDomains = async (domains: string[]): Promise<void> => {
  const queue = [...domains];
  let activeRequests = 0;

  const processDomain = async (domain: string) => {
    activeRequests++;

    console.log(`Checking domain: ${domain}`);
    const robotsTxt = await fetchRobotsTxt(domain);
    let foundPriceUrls: string[] = [];
    let checkedUrls: string[] = [];

    if (robotsTxt) {
      const sitemapUrls = parseSitemapUrls(robotsTxt);
      if (sitemapUrls.length > 0) {
        for (const sitemapUrl of sitemapUrls) {
          const priceUrls = await fetchAndParseSitemap(sitemapUrl);
          foundPriceUrls = foundPriceUrls.concat(priceUrls);
        }
      }
    }

    if (foundPriceUrls.length === 0) {
      checkedUrls = await checkFallbackUrls(domain);
    }

    saveDomainResults(domain, foundPriceUrls, checkedUrls);
    console.log(`Results saved for domain: ${domain}`);

    activeRequests--;
  };

  const runBatch = async () => {
    const domainBatch = queue.splice(0, MAX_CONCURRENT_REQUESTS);

    await Promise.all(domainBatch.map((domain) => processDomain(domain)));

    if (queue.length > 0) {
      await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY)); // Rate limit between batches
      await runBatch(); // Continue processing remaining domains
    }
  };

  await runBatch();
  console.log('All domains processed and results saved.');
};

// Main function to download, unzip, parse CSV, and process domains
const main = async () => {
  try {
    await downloadAndUnzip(); // Download and unzip the file
    const domains = await parseDomainsFromCSV(EXTRACTED_CSV_PATH); // Parse domains from CSV
    await processDomains(domains); // Process domains in batches
  } catch (error) {
    console.error('Error in processing domains:', error);
  }
};

main();
