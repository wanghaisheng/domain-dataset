const fs = require('fs');
const path = require('path');
const axios = require('axios');
const unzipper = require('unzipper');
const csv = require('csv-parser');
const crypto = require('crypto');
const { createClient } = require('@libsql/client');
require('dotenv').config();

const TRANC0_URL = 'https://tranco-list.eu/top-1m.csv.zip';
const ZIP_FILE_PATH = './tranco.zip';
const EXTRACTED_CSV_PATH = './top-1m.csv';

const dbUrl = process.env.TURSO_DB_URL;
const authToken = process.env.TURSO_DB_AUTH_TOKEN;

if (!dbUrl || !authToken) {
  console.error('Missing TURSO_DB_URL or TURSO_DB_AUTH_TOKEN in .env file');
  process.exit(1);
}

const client = createClient({
  url: dbUrl,
  authToken: authToken,
});

async function downloadZipFile(url, outputPath) {
  console.log(`Downloading zip file from ${url}...`);
  const writer = fs.createWriteStream(outputPath);

  const response = await axios({
    url,
    method: 'GET',
    responseType: 'stream',
  });

  response.data.pipe(writer);

  return new Promise((resolve, reject) => {
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
}

async function unzipFile(zipPath, outputDir) {
  console.log(`Extracting zip file: ${zipPath}`);
  return fs
    .createReadStream(zipPath)
    .pipe(unzipper.Extract({ path: outputDir }))
    .promise();
}

function generateDomainId(domainName) {
  return crypto.createHash('sha256').update(domainName).digest('hex');
}

async function ensureDomainExists(domain) {
  const domainId = generateDomainId(domain);
  const timestamp = new Date().toISOString();

  // Check if the domain exists
  const result = await client.execute({
    sql: 'SELECT id FROM domains WHERE id = ?',
    args: [domainId],
  });

  if (result.rows.length > 0) {
    return domainId; // Domain already exists
  }

  // Insert the domain
  await client.execute({
    sql: `INSERT INTO domains (id, domain, tld, createdAt, updatedAt)
          VALUES (?, ?, ?, ?, ?)`,
    args: [
      domainId,
      domain,
      domain.split('.').pop(),
      timestamp,
      timestamp,
    ],
  });

  return domainId;
}

async function insertRank(domainId, rank) {
  const timestamp = new Date().toISOString();

  await client.execute({
    sql: `INSERT OR REPLACE INTO tranco_rankings (domain_id, rank, createdAt, updatedAt)
          VALUES (?, ?, ?, ?)`,
    args: [domainId, rank, timestamp, timestamp],
  });
}

async function processCsvFile(filePath) {
  const startTime = Date.now();
  let totalProcessed = 0;

  const processStream = new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', async (row) => {
        const rank = parseInt(row.rank);
        const domain = row.domain?.trim();

        if (!domain || isNaN(rank)) return;

        try {
          const domainId = await ensureDomainExists(domain);
          await insertRank(domainId, rank);
          totalProcessed += 1;
        } catch (error) {
          console.error(`Error processing domain ${domain}:`, error);
        }
      })
      .on('end', () => {
        console.log(`Completed processing ${totalProcessed} domains.`);
        const elapsedTime = (Date.now() - startTime) / 1000;
        console.log(`Elapsed Time: ${elapsedTime.toFixed(2)} seconds`);
        resolve();
      })
      .on('error', reject);
  });

  await processStream;
}

(async () => {
  try {
    console.log('Starting Tranco update workflow...');

    // Step 1: Download the zip file
    await downloadZipFile(TRANC0_URL, ZIP_FILE_PATH);
    console.log('Zip file downloaded successfully.');

    // Step 2: Extract the CSV
    await unzipFile(ZIP_FILE_PATH, './');
    console.log('Zip file extracted successfully.');

    // Step 3: Process the CSV file
    await processCsvFile(EXTRACTED_CSV_PATH);

    // Cleanup
    fs.unlinkSync(ZIP_FILE_PATH);
    fs.unlinkSync(EXTRACTED_CSV_PATH);
    console.log('Temporary files deleted.');

    console.log('Tranco update completed successfully.');
  } catch (error) {
    console.error('Error in Tranco update workflow:', error);
    process.exit(1);
  } finally {
    await client.close();
  }
})();
