-- Create the Domains table to store domain information
CREATE TABLE IF NOT EXISTS domains (
  id VARCHAR(255) PRIMARY KEY,  -- Hash ID of the domain (VARCHAR for primary key)
  domain VARCHAR(255) NOT NULL UNIQUE,  -- Domain name
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the domain was created
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  -- Date when the domain info was last updated
);

-- Create the Domain Pages table to store basic page information for each domain
CREATE TABLE IF NOT EXISTS domain_pages (
  id INT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each page
  domain_id VARCHAR(255) NOT NULL,  -- Hash ID of the domain (VARCHAR for foreign key)
  page_title TEXT,  -- Page title
  page_description TEXT,  -- Page description
  page_text TEXT,  -- Text content of the page
  page_md TEXT,  -- Markdown content of the page
  h1_title TEXT,  -- H1 title of the page
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains(id)  -- Link to the domains table
);

-- Create the Google Index Info table to store Google index information for each domain
CREATE TABLE IF NOT EXISTS google_index_info (
  id INT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each record
  domain_id VARCHAR(255) NOT NULL,  -- Hash ID of the domain
  index_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the domain was indexed by Google
  about_the_source TEXT,  -- Information about the source of the domain
  in_their_own_words TEXT,  -- Domain's own description (if available)
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains(id)  -- Link to the domains table
);

-- Create the Product or Service table to store information about products or services offered by a domain
CREATE TABLE IF NOT EXISTS product_service (
  id INT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each record
  domain_id VARCHAR(255) NOT NULL,  -- Hash ID of the domain
  lang VARCHAR(50),  -- Language of the product or service
  currency VARCHAR(10),  -- Currency used for the product or service
  links TEXT,  -- Relevant links (e.g., product page)
  prices TEXT,  -- Prices associated with the product or service
  price_plans TEXT,  -- Price plans available for the product or service
  html_path TEXT,  -- Path to the HTML version of the product or service
  md_path TEXT,  -- Path to the markdown version of the product or service
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains(id)  -- Link to the domains table
);

-- Create the Tranco Ranking table to store Tranco ranking data for domains
CREATE TABLE IF NOT EXISTS tranco_rankings (
  id INT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each ranking record
  domain_id VARCHAR(255) NOT NULL,  -- Hash ID of the domain
  rank INT,  -- Tranco rank of the domain
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains(id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Create the DomCop Ranking table to store DomCop ranking data for domains
CREATE TABLE IF NOT EXISTS domcop_rankings (
  id INT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each ranking record
  domain_id VARCHAR(255) NOT NULL,  -- Hash ID of the domain
  rank INT,  -- DomCop rank of the domain
  open_page_rank FLOAT,  -- DomCop open page rank of the domain
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains(id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Create the Majestic Ranking table to store Majestic ranking data for domains
CREATE TABLE IF NOT EXISTS majestic_rankings (
  id INT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each ranking record
  domain_id VARCHAR(255) NOT NULL,  -- Hash ID of the domain
  global_rank INT,  -- Global rank of the domain
  tld_rank INT,  -- TLD rank of the domain
  tld VARCHAR(10),  -- Top-level domain (TLD) of the domain
  ref_subnets INT,  -- Number of referring subnets
  ref_ips INT,  -- Number of referring IPs
  idn_domain VARCHAR(255),  -- IDN domain (if applicable)
  idn_tld VARCHAR(10),  -- IDN TLD (if applicable)
  prev_global_rank INT,  -- Previous global rank (if applicable)
  prev_tld_rank INT,  -- Previous TLD rank (if applicable)
  prev_ref_subnets INT,  -- Previous referring subnets (if applicable)
  prev_ref_ips INT,  -- Previous referring IPs (if applicable)
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains(id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Create the Umbrella Ranking table to store Umbrella ranking data for domains
CREATE TABLE IF NOT EXISTS umbrella_rankings (
  id INT PRIMARY KEY AUTO_INCREMENT,  -- Unique identifier for each ranking record
  domain_id VARCHAR(255) NOT NULL,  -- Hash ID of the domain
  rank INT,  -- Umbrella rank of the domain
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  -- Last update timestamp
  FOREIGN KEY (domain_id) REFERENCES domains(id),  -- Link to the domains table
  UNIQUE(domain_id, updatedAt)  -- Ensure uniqueness for each domain and ranking date
);

-- Add index on domain_id and updatedAt for faster lookups
CREATE INDEX IF NOT EXISTS idx_tranco_rankings_domain_updated_at ON tranco_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_domcop_rankings_domain_updated_at ON domcop_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_majestic_rankings_domain_updated_at ON majestic_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_umbrella_rankings_domain_updated_at ON umbrella_rankings (domain_id, updatedAt);

-- Weekly Domain Ranking Report View
DROP VIEW IF EXISTS weekly_domain_ranking_report;
CREATE VIEW weekly_domain_ranking_report AS
WITH ranked_data AS (
  SELECT 
    domain_id,
    tranco_rank AS current_tranco_rank,
    LAG(tranco_rank) OVER (PARTITION BY domain_id ORDER BY DATE_FORMAT(updatedAt, '%Y-%u')) AS previous_tranco_rank,
    domcop_rank AS current_domcop_rank,
    LAG(domcop_rank) OVER (PARTITION BY domain_id ORDER BY DATE_FORMAT(updatedAt, '%Y-%u')) AS previous_domcop_rank,
    majestic_rank AS current_majestic_rank,
    LAG(majestic_rank) OVER (PARTITION BY domain_id ORDER BY DATE_FORMAT(updatedAt, '%Y-%u')) AS previous_majestic_rank,
    umbrella_rank AS current_umbrella_rank,
    LAG(umbrella_rank) OVER (PARTITION BY domain_id ORDER BY DATE_FORMAT(updatedAt, '%Y-%u')) AS previous_umbrella_rank
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
FROM ranked_data;
