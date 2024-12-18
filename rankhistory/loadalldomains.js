# combine all historical csv and get unique domain as csv

const fs = require('fs');
const csv = require('csv-parser');
const crypto = require('crypto');
const { createClient } = require('@libsql/client');
require('dotenv').config();

const dbUrl = process.env.TURSO_DB_URL;
const authToken = process.env.TURSO_DB_AUTH_TOKEN;
const BATCH_SIZE = 100; // Reduced batch size
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 1 second
const RATE_LIMIT_DELAY = 500; // 500ms between batches

if (!dbUrl || !authToken) {
  console.error('Missing TURSO_DB_URL or TURSO_DB_AUTH_TOKEN in .env file');
  process.exit(1);
}

const client = createClient({
  url: dbUrl,
  authToken: authToken
});

function generateDomainId(domainName) {
  return crypto.createHash('sha256').update(domainName).digest('hex');
}

// Sleep function for delays
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function batchInsertDomains(domains, retryCount = 0) {
  if (domains.length === 0) return;

  try {
    const valuePlaceholders = domains
      .map(() => "(?, ?, ?, ?, ?)")
      .join(", ");

    const values = domains.flatMap(({ id, domain, tld, timestamp }) => [
      id,
      domain,
      tld,
      timestamp,
      timestamp
    ]);

    await client.execute({
      sql: `INSERT OR IGNORE INTO domains (id, domain, tld, createdAt, updatedAt) 
            VALUES ${valuePlaceholders}`,
      args: values
    });

    console.log(`Inserted batch of ${domains.length} domains`);
    
    // Add delay between successful batches to prevent rate limiting
    await sleep(RATE_LIMIT_DELAY);

  } catch (error) {
    console.error(`Error inserting batch (attempt ${retryCount + 1}):`, error.message);

    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying in ${RETRY_DELAY}ms...`);
      await sleep(RETRY_DELAY * (retryCount + 1)); // Exponential backoff
      return batchInsertDomains(domains, retryCount + 1);
    }

    // If batch is too large, split it and retry
    if (domains.length > 10) {
      console.log('Splitting batch and retrying...');
      const mid = Math.floor(domains.length / 2);
      const firstHalf = domains.slice(0, mid);
      const secondHalf = domains.slice(mid);

      await batchInsertDomains(firstHalf, 0);
      await batchInsertDomains(secondHalf, 0);
      return;
    }

    throw error;
  }
}

async function processCsvFile(filePath) {
  let batch = [];
  let totalProcessed = 0;
  let startTime = Date.now();
  let failedDomains = [];

  const processStream = () => {
    return new Promise((resolve, reject) => {
      const stream = fs.createReadStream(filePath)
        .pipe(csv())
        .on('error', reject);

      stream.on('data', async (row) => {
        const domainName = row.domain;
        if (!domainName) return;

        batch.push({
          id: generateDomainId(domainName),
          domain: domainName,
          tld: domainName.split('.').pop(),
          timestamp: new Date().toISOString()
        });

        if (batch.length >= BATCH_SIZE) {
          stream.pause();
          try {
            await batchInsertDomains(batch);
            totalProcessed += batch.length;
            
            const elapsed = (Date.now() - startTime) / 1000;
            const rate = Math.round(totalProcessed / elapsed);
            console.log(`Processed ${totalProcessed} domains (${rate} domains/sec)`);
            
            batch = [];
          } catch (error) {
            console.error('Failed to process batch, storing for retry');
            failedDomains.push(...batch);
            batch = [];
          }
          stream.resume();
        }
      });

      stream.on('end', async () => {
        try {
          if (batch.length > 0) {
            await batchInsertDomains(batch);
            totalProcessed += batch.length;
          }

          // Retry failed domains
          if (failedDomains.length > 0) {
            console.log(`Retrying ${failedDomains.length} failed domains...`);
            for (let i = 0; i < failedDomains.length; i += 10) {
              const smallBatch = failedDomains.slice(i, i + 10);
              try {
                await batchInsertDomains(smallBatch);
                totalProcessed += smallBatch.length;
              } catch (error) {
                console.error(`Failed to process retry batch: ${error.message}`);
              }
              await sleep(RATE_LIMIT_DELAY);
            }
          }

          const elapsed = (Date.now() - startTime) / 1000;
          console.log(`\nCompleted processing ${totalProcessed} domains in ${elapsed.toFixed(2)} seconds`);
          console.log(`Average rate: ${Math.round(totalProcessed / elapsed)} domains/sec`);
          
          resolve();
        } catch (error) {
          reject(error);
        }
      });
    });
  };

  try {
    await processStream();
  } catch (error) {
    console.error('Error processing file:', error);
  } finally {
    await client.close();
  }
}

// Add memory usage monitoring
function logMemoryUsage() {
  const used = process.memoryUsage();
  console.log('Memory usage:');
  for (let key in used) {
    console.log(`${key}: ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB`);
  }
}

// Create a backup of failed domains
function saveFailedDomains(domains) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `failed_domains_${timestamp}.json`;
  fs.writeFileSync(filename, JSON.stringify(domains, null, 2));
  console.log(`Failed domains saved to ${filename}`);
}

// Start processing with error handling
(async () => {
  try {
    console.log('Starting import process...');
    logMemoryUsage();
    
    await processCsvFile('domain_ids.csv');
    
    console.log('\nFinal memory usage:');
    logMemoryUsage();
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
})();
