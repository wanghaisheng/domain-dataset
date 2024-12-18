# download from https://github.com/adysec/top_1m_domains/tree/main
# use file name to track date
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const unzipper = require('unzipper');
const csv = require('csv-parser');
const { createClient } = require('@libsql/client');  // Import LibSQL client

// Environment variables for Turso DB connection
const dbUrl = process.env.TURSO_DB_URL;
const authToken = process.env.TURSO_DB_AUTH_TOKEN;

// Define your LibSQL client connection
const client = createClient({
  url: dbUrl,
  authToken: authToken
});

// Define file paths for CSV files
const FILE_PATHS = {
  umbrellaCsv: './umbrella-top-1m-2024-06-01.csv',
  majesticCsv: './majestic-top-1m-2024-06-01.csv',
  trancoCsv: './tranco-top-1m-2024-06-01.csv',
};

// Define the URLs for the sources
const URLs = {
  tranco: 'https://tranco-list.eu/top-1m.csv.zip',
  majestic: 'https://downloads.majestic.com/majestic_million.csv',
  umbrella: 'https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip',
};

// BATCH_SIZE for inserting domains in batches
const BATCH_SIZE = 1000; // Adjust based on your needs

// Function to extract date from CSV file name
function extractDateFromFileName(fileName) {
  const dateMatch = fileName.match(/(\d{4}-\d{2}-\d{2})/); // Matches date in format YYYY-MM-DD
  return dateMatch ? dateMatch[0] : new Date().toISOString(); // Default to current date if no match
}

// Function to download zip file from URL
async function downloadZipFile(url, destination) {
  const writer = fs.createWriteStream(destination);
  const response = await axios.get(url, { responseType: 'stream' });
  response.data.pipe(writer);
  return new Promise((resolve, reject) => {
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
}

// Function to unzip the file
async function unzipFile(zipPath, extractTo) {
  await fs.createReadStream(zipPath)
    .pipe(unzipper.Extract({ path: extractTo }))
    .promise();
}

// Function to get domain_id based on the domain
async function getDomainId(domain) {
  const query = 'SELECT id FROM domains WHERE domain = ?';
  const result = await client.execute(query, [domain]);

  if (result.length > 0) {
    return result[0].id; // Domain found, return its id
  } else {
    const domainId = Buffer.from(domain).toString('base64'); // Generate a unique ID
    const insertQuery = 'INSERT INTO domains (id, domain) VALUES (?, ?)';
    await client.execute(insertQuery, [domainId, domain]);
    return domainId;
  }
}

// Function to insert ranking data into the database
async function insertRankData(tableName, domainId, data, updatedAt) {
  const queryMap = {
    umbrella: 'INSERT OR REPLACE INTO umbrella_rankings (domain_id, rank, updatedAt) VALUES (?, ?, ?)',
    majestic: `INSERT OR REPLACE INTO majestic_rankings (domain_id, global_rank, tld_rank, tld, ref_subnets, ref_ips, idn_domain, idn_tld, prev_global_rank, prev_tld_rank, prev_ref_subnets, prev_ref_ips, updatedAt)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    tranco: 'INSERT OR REPLACE INTO tranco_rankings (domain_id, rank, updatedAt) VALUES (?, ?, ?)',
  };

  let query = queryMap[tableName];
  const values = [domainId, ...data, updatedAt];
  
  await client.execute(query, values);
}

// Function to process CSV file in batches
async function processCsvFile(filePath, tableName) {
  const startTime = Date.now();
  let totalProcessed = 0;
  const batchData = [];
  const updatedAt = extractDateFromFileName(path.basename(filePath));  // Extract date from file name

  const processStream = new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', async (row) => {
        const domain = row.domain?.trim();
        if (!domain) return;

        try {
          // Get or create domain_id based on the domain
          let domainId = await getDomainId(domain);

          if (tableName === 'umbrella') {
            const rank = row.rank ? parseInt(row.rank) : null;
            batchData.push([domainId, rank]);
          } else if (tableName === 'majestic') {
            batchData.push([
              domainId,
              row.GlobalRank,
              row.TldRank,
              row.TLD,
              row.RefSubNets,
              row.RefIPs,
              row.IDN_Domain,
              row.IDN_TLD,
              row.PrevGlobalRank,
              row.PrevTldRank,
              row.PrevRefSubNets,
              row.PrevRefIPs,
            ]);
          } else if (tableName === 'tranco') {
            const rank = row.rank ? parseInt(row.rank) : null;
            batchData.push([domainId, rank]);
          }

          totalProcessed += 1;

          // Insert in batches
          if (batchData.length >= BATCH_SIZE) {
            await insertBatch(tableName, batchData, updatedAt);
            batchData.length = 0; // Reset batch
          }
        } catch (error) {
          console.error(`Error processing domain ${domain}:`, error);
        }
      })
      .on('end', async () => {
        if (batchData.length > 0) {
          await insertBatch(tableName, batchData, updatedAt);
        }
        console.log(`Completed processing ${totalProcessed} domains.`);
        const elapsedTime = (Date.now() - startTime) / 1000;
        console.log(`Elapsed Time: ${elapsedTime.toFixed(2)} seconds`);
        resolve();
      })
      .on('error', reject);
  });

  await processStream;
}

// Function to insert data in batches
async function insertBatch(tableName, batchData, updatedAt) {
  const queryMap = {
    umbrella_rankings: 'INSERT OR REPLACE INTO umbrella_rankings (domain_id, rank, updatedAt) VALUES (?, ?, ?)',
    majestic_rankings: `INSERT OR REPLACE INTO majestic_rankings (domain_id, global_rank, tld_rank, tld, ref_subnets, ref_ips, idn_domain, idn_tld, prev_global_rank, prev_tld_rank, prev_ref_subnets, prev_ref_ips, updatedAt)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    tranco_rankings: 'INSERT OR REPLACE INTO tranco_rankings (domain_id, rank, updatedAt) VALUES (?, ?, ?)',
  };

  const timestamp = updatedAt; // Use extracted date as timestamp
  const query = queryMap[tableName];
  const batchValues = batchData.map(item => [...item, timestamp]);  // Append timestamp to batch
  await client.executeBatch(query, batchValues);
}

// Function to update ranking data from different sources
async function updateRankingForSource(source) {
  console.log(`Starting update for ${source}...`);

  await downloadZipFile(URLs[source], FILE_PATHS[source]);
  console.log(`${source} zip file downloaded successfully.`);

  if (source === 'umbrella') {
    await unzipFile(FILE_PATHS[source], './');
    console.log(`${source} zip file extracted successfully.`);
  }

  if (source === 'umbrella') {
    await processCsvFile(FILE_PATHS.umbrellaCsv, 'umbrella');
  } else if (source === 'majestic') {
    await processCsvFile(FILE_PATHS.majesticCsv, 'majestic');
  } else if (source === 'tranco') {
    await processCsvFile(FILE_PATHS.trancoCsv, 'tranco');
  }

  console.log(`Update for ${source} completed.`);
}

// Main function to update rankings
async function updateRankings() {
  await updateRankingForSource('umbrella');
  await updateRankingForSource('majestic');
  await updateRankingForSource('tranco');
}

// Run the update process
updateRankings()
  .then(() => console.log('Ranking data updated successfully.'))
  .catch(error => console.error('Error updating ranking data:', error));
