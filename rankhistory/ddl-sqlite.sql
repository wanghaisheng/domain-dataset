-- Create the Domains table to store domain information
CREATE TABLE IF NOT EXISTS domains (
  id TEXT PRIMARY KEY,  -- Hash ID of the domain
  domain TEXT NOT NULL UNIQUE,  -- Domain name
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the domain was created
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Date when the domain info was last updated
);

-- Create the Domain Pages table to store basic page information for each domain
CREATE TABLE IF NOT EXISTS domain_pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique identifier for each page
  domain_id TEXT NOT NULL,  -- Hash ID of the domain
  page_title TEXT,  -- Page title
  page_description TEXT,  -- Page description
  page_text TEXT,  -- Text content of the page
  page_md TEXT,  -- Markdown content of the page
  h1_title TEXT,  -- H1 title of the page
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains (id)  -- Link to the domains table
);

-- Create the Google Index Info table to store Google index information for each domain
CREATE TABLE IF NOT EXISTS google_index_info (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique identifier for each record
  domain_id TEXT NOT NULL,  -- Hash ID of the domain
  index_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the domain was indexed by Google
  about_the_source TEXT,  -- Information about the source of the domain
  in_their_own_words TEXT,  -- Domain's own description (if available)
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains (id)  -- Link to the domains table
);

-- Create the Product or Service table to store information about products or services offered by a domain
CREATE TABLE IF NOT EXISTS product_service (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique identifier for each record
  domain_id TEXT NOT NULL,  -- Hash ID of the domain
  lang TEXT,  -- Language of the product or service
  currency TEXT,  -- Currency used for the product or service
  links TEXT,  -- Relevant links (e.g., product page)
  prices TEXT,  -- Prices associated with the product or service
  price_plans TEXT,  -- Price plans available for the product or service
  html_path TEXT,  -- Path to the HTML version of the product or service
  md_path TEXT,  -- Path to the markdown version of the product or service
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains (id)  -- Link to the domains table
);

-- Create the Tranco Ranking table to store Tranco ranking data for domains
CREATE TABLE IF NOT EXISTS tranco_rankings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique identifier for each ranking record
  domain_id TEXT NOT NULL,  -- Hash ID of the domain
  rank INTEGER,  -- Tranco rank of the domain
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains (id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Create the DomCop Ranking table to store DomCop ranking data for domains
CREATE TABLE IF NOT EXISTS domcop_rankings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique identifier for each ranking record
  domain_id TEXT NOT NULL,  -- Hash ID of the domain
  rank INTEGER,  -- DomCop rank of the domain
  open_page_rank FLOAT,  -- DomCop open page rank of the domain
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains (id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Create the Majestic Ranking table to store Majestic ranking data for domains
CREATE TABLE IF NOT EXISTS majestic_rankings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique identifier for each ranking record
  domain_id TEXT NOT NULL,  -- Hash ID of the domain
  global_rank INTEGER,  -- Global rank of the domain
  tld_rank INTEGER,  -- TLD rank of the domain
  tld TEXT,  -- Top-level domain (TLD) of the domain
  ref_subnets INTEGER,  -- Number of referring subnets
  ref_ips INTEGER,  -- Number of referring IPs
  idn_domain TEXT,  -- IDN domain (if applicable)
  idn_tld TEXT,  -- IDN TLD (if applicable)
  prev_global_rank INTEGER,  -- Previous global rank (if applicable)
  prev_tld_rank INTEGER,  -- Previous TLD rank (if applicable)
  prev_ref_subnets INTEGER,  -- Previous referring subnets (if applicable)
  prev_ref_ips INTEGER,  -- Previous referring IPs (if applicable)
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains (id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Create the Umbrella Ranking table to store Umbrella ranking data for domains
CREATE TABLE IF NOT EXISTS umbrella_rankings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique identifier for each ranking record
  domain_id TEXT NOT NULL,  -- Hash ID of the domain
  rank INTEGER,  -- Umbrella rank of the domain
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains (id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Add index on domain_id and updatedAt for faster lookups
CREATE INDEX IF NOT EXISTS idx_tranco_rankings_domain_updated_at ON tranco_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_domcop_rankings_domain_updated_at ON domcop_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_majestic_rankings_domain_updated_at ON majestic_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_umbrella_rankings_domain_updated_at ON umbrella_rankings (domain_id, updatedAt);


DROP VIEW IF EXISTS weekly_domain_ranking_report;
CREATE VIEW weekly_domain_ranking_report AS
WITH ranked_data AS (
  SELECT 
    domain_id,
    tranco_rank AS current_tranco_rank,
    LAG(tranco_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_tranco_rank,
    domcop_rank AS current_domcop_rank,
    LAG(domcop_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_domcop_rank,
    majestic_rank AS current_majestic_rank,
    LAG(majestic_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_majestic_rank,
    umbrella_rank AS current_umbrella_rank,
    LAG(umbrella_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_umbrella_rank
  FROM weekly_domain_ranking_changes
)
SELECT 
  domain_id,
  current_tranco_rank,
  previous_tranco_rank,
  CASE 
    WHEN current_tranco_rank IS NOT NULL AND previous_tranco_rank IS NOT NULL
    THEN current_tranco_rank - previous_tranco_rank
    ELSE NULL
  END AS tranco_rank_change,
  current_domcop_rank,
  previous_domcop_rank,
  CASE 
    WHEN current_domcop_rank IS NOT NULL AND previous_domcop_rank IS NOT NULL
    THEN current_domcop_rank - previous_domcop_rank
    ELSE NULL
  END AS domcop_rank_change,
  current_majestic_rank,
  previous_majestic_rank,
  CASE 
    WHEN current_majestic_rank IS NOT NULL AND previous_majestic_rank IS NOT NULL
    THEN current_majestic_rank - previous_majestic_rank
    ELSE NULL
  END AS majestic_rank_change,
  current_umbrella_rank,
  previous_umbrella_rank,
  CASE 
    WHEN current_umbrella_rank IS NOT NULL AND previous_umbrella_rank IS NOT NULL
    THEN current_umbrella_rank - previous_umbrella_rank
    ELSE NULL
  END AS umbrella_rank_change
FROM ranked_data
WHERE previous_tranco_rank IS NOT NULL 
   OR previous_domcop_rank IS NOT NULL
   OR previous_majestic_rank IS NOT NULL
   OR previous_umbrella_rank IS NOT NULL;


DROP VIEW IF EXISTS monthly_domain_ranking_report;
CREATE VIEW monthly_domain_ranking_report AS
WITH ranked_data AS (
  SELECT 
    domain_id,
    tranco_rank,
    LAG(tranco_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%m', updatedAt)) AS prev_tranco_rank,
    domcop_rank,
    LAG(domcop_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%m', updatedAt)) AS prev_domcop_rank,
    majestic_rank,
    LAG(majestic_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%m', updatedAt)) AS prev_majestic_rank,
    umbrella_rank,
    LAG(umbrella_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%m', updatedAt)) AS prev_umbrella_rank
  FROM monthly_domain_ranking_changes
)
SELECT 
  domain_id,
  tranco_rank AS current_tranco_rank,
  prev_tranco_rank AS previous_tranco_rank,
  CASE 
    WHEN tranco_rank IS NOT NULL AND prev_tranco_rank IS NOT NULL
    THEN tranco_rank - prev_tranco_rank
    ELSE NULL
  END AS tranco_rank_change,
  domcop_rank AS current_domcop_rank,
  prev_domcop_rank AS previous_domcop_rank,
  CASE 
    WHEN domcop_rank IS NOT NULL AND prev_domcop_rank IS NOT NULL
    THEN domcop_rank - prev_domcop_rank
    ELSE NULL
  END AS domcop_rank_change,
  majestic_rank AS current_majestic_rank,
  prev_majestic_rank AS previous_majestic_rank,
  CASE 
    WHEN majestic_rank IS NOT NULL AND prev_majestic_rank IS NOT NULL
    THEN majestic_rank - prev_majestic_rank
    ELSE NULL
  END AS majestic_rank_change,
  umbrella_rank AS current_umbrella_rank,
  prev_umbrella_rank AS previous_umbrella_rank,
  CASE 
    WHEN umbrella_rank IS NOT NULL AND prev_umbrella_rank IS NOT NULL
    THEN umbrella_rank - prev_umbrella_rank
    ELSE NULL
  END AS umbrella_rank_change
FROM ranked_data
WHERE 
  (tranco_rank IS NOT NULL AND prev_tranco_rank IS NOT NULL)
  OR (domcop_rank IS NOT NULL AND prev_domcop_rank IS NOT NULL)
  OR (majestic_rank IS NOT NULL AND prev_majestic_rank IS NOT NULL)
  OR (umbrella_rank IS NOT NULL AND prev_umbrella_rank IS NOT NULL);



DROP VIEW IF EXISTS weekly_domain_ranking_report;
CREATE VIEW weekly_domain_ranking_report AS
WITH ranked_data AS (
  SELECT 
    domain_id,
    tranco_rank AS current_tranco_rank,
    LAG(tranco_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_tranco_rank,
    domcop_rank AS current_domcop_rank,
    LAG(domcop_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_domcop_rank,
    majestic_rank AS current_majestic_rank,
    LAG(majestic_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_majestic_rank,
    umbrella_rank AS current_umbrella_rank,
    LAG(umbrella_rank) OVER (PARTITION BY domain_id ORDER BY strftime('%Y-%W', updatedAt)) AS previous_umbrella_rank
  FROM weekly_domain_ranking_changes
)
SELECT 
  domain_id,
  current_tranco_rank,
  previous_tranco_rank,
  CASE 
    WHEN current_tranco_rank IS NOT NULL AND previous_tranco_rank IS NOT NULL
    THEN current_tranco_rank - previous_tranco_rank
    ELSE NULL
  END AS tranco_rank_change,
  current_domcop_rank,
  previous_domcop_rank,
  CASE 
    WHEN current_domcop_rank IS NOT NULL AND previous_domcop_rank IS NOT NULL
    THEN current_domcop_rank - previous_domcop_rank
    ELSE NULL
  END AS domcop_rank_change,
  current_majestic_rank,
  previous_majestic_rank,
  CASE 
    WHEN current_majestic_rank IS NOT NULL AND previous_majestic_rank IS NOT NULL
    THEN current_majestic_rank - previous_majestic_rank
    ELSE NULL
  END AS majestic_rank_change,
  current_umbrella_rank,
  previous_umbrella_rank,
  CASE 
    WHEN current_umbrella_rank IS NOT NULL AND previous_umbrella_rank IS NOT NULL
    THEN current_umbrella_rank - previous_umbrella_rank
    ELSE NULL
  END AS umbrella_rank_change
FROM ranked_data
WHERE previous_tranco_rank IS NOT NULL 
   OR previous_domcop_rank IS NOT NULL
   OR previous_majestic_rank IS NOT NULL
   OR previous_umbrella_rank IS NOT NULL;



