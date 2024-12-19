USE test;
-- Create the Tranco Ranking table to store Tranco ranking data for domains
CREATE TABLE IF NOT EXISTS
  tranco_rankings (
    id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for each ranking record
    domain_id VARCHAR(255) NOT NULL, -- Hash ID of the domain
    `rank` INT, -- Tranco rank of the domain
    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    UNIQUE (domain_id, updatedAt), -- Ensure uniqueness for each domain and ranking date
    FOREIGN KEY (domain_id) REFERENCES `domains` (`id`) -- Link to the domains table
  );

-- Create the DomCop Ranking table to store DomCop ranking data for domains
CREATE TABLE IF NOT EXISTS
  domcop_rankings (
    id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for each ranking record
    domain_id VARCHAR(255) NOT NULL, -- Hash ID of the domain
    `rank` INT, -- DomCop rank of the domain
    open_page_rank FLOAT, -- DomCop open page rank of the domain
    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    UNIQUE (domain_id, updatedAt), -- Ensure uniqueness for each domain and ranking date
    FOREIGN KEY (domain_id) REFERENCES `domains` (`id`) -- Link to the domains table
  );

-- Create the Majestic Ranking table to store Majestic ranking data for domains
CREATE TABLE IF NOT EXISTS majestic_rankings (
  id INT AUTO_INCREMENT PRIMARY KEY,  -- Unique identifier for each ranking record
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
  UNIQUE (domain_id, updatedAt),  -- Ensure uniqueness for each domain and ranking date
    FOREIGN KEY (domain_id) REFERENCES `domains` (`id`) -- Link to the domains table
);

-- Create the Umbrella Ranking table to store Umbrella ranking data for domains
CREATE TABLE IF NOT EXISTS
  umbrella_rankings (
    id INT AUTO_INCREMENT PRIMARY KEY, -- Unique identifier for each ranking record
    domain_id VARCHAR(255) NOT NULL, -- Hash ID of the domain
    `rank` INT, -- Umbrella rank of the domain
    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, -- Last update timestamp
    UNIQUE (domain_id, updatedAt), -- Ensure uniqueness for each domain and ranking date
    FOREIGN KEY (domain_id) REFERENCES `domains` (`id`) -- Link to the domains table
  );

-- Add index on domain_id and updatedAt for faster lookups
CREATE INDEX IF NOT EXISTS idx_tranco_rankings_domain_updated_at ON tranco_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_domcop_rankings_domain_updated_at ON domcop_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_majestic_rankings_domain_updated_at ON majestic_rankings (domain_id, updatedAt);
CREATE INDEX IF NOT EXISTS idx_umbrella_rankings_domain_updated_at ON umbrella_rankings (domain_id, updatedAt);



use test;
DROP VIEW IF EXISTS weekly_domain_ranking_report;
CREATE VIEW
  `weekly_domain_ranking_report` AS
WITH
  `ranked_data` AS (
    SELECT
      `domain_id`,
      `tranco_rank` AS `current_tranco_rank`,
      LAG(`tranco_rank`) OVER (
        PARTITION BY
          `domain_id`
        ORDER BY
          strftime ('%Y-%W', `updatedAt`)
      ) AS `previous_tranco_rank`,
      `domcop_rank` AS `current_domcop_rank`,
      LAG(`domcop_rank`) OVER (
        PARTITION BY
          `domain_id`
        ORDER BY
          strftime ('%Y-%W', `updatedAt`)
      ) AS `previous_domcop_rank`,
      `majestic_rank` AS `current_majestic_rank`,
      LAG(`majestic_rank`) OVER (
        PARTITION BY
          `domain_id`
        ORDER BY
          strftime ('%Y-%W', `updatedAt`)
      ) AS `previous_majestic_rank`,
      `umbrella_rank` AS `current_umbrella_rank`,
      LAG(`umbrella_rank`) OVER (
        PARTITION BY
          `domain_id`
        ORDER BY
          strftime ('%Y-%W', `updatedAt`)
      ) AS `previous_umbrella_rank`
    FROM
      `existing_table_name`
  )
SELECT
  `domain_id`,
  `current_tranco_rank`,
  `previous_tranco_rank`,
  CASE
    WHEN `current_tranco_rank` IS NOT NULL
    AND `previous_tranco_rank` IS NOT NULL THEN `current_tranco_rank` - `previous_tranco_rank`
    ELSE NULL
  END AS `tranco_rank_change`,
  `current_domcop_rank`,
  `previous_domcop_rank`,
  CASE
    WHEN `current_domcop_rank` IS NOT NULL
    AND `previous_domcop_rank` IS NOT NULL THEN `current_domcop_rank` - `previous_domcop_rank`
    ELSE NULL
  END AS `domcop_rank_change`,
  `current_majestic_rank`,
  `previous_majestic_rank`,
  CASE
    WHEN `current_majestic_rank` IS NOT NULL
    AND `previous_majestic_rank` IS NOT NULL THEN `current_majestic_rank` - `previous_majestic_rank`
    ELSE NULL
  END AS `majestic_rank_change`,
  `current_umbrella_rank`,
  `previous_umbrella_rank`,
  CASE
    WHEN `current_umbrella_rank` IS NOT NULL
    AND `previous_umbrella_rank` IS NOT NULL THEN `current_umbrella_rank` - `previous_umbrella_rank`
    ELSE NULL
  END AS `umbrella_rank_change`
FROM
  `ranked_data`
WHERE
  `previous_tranco_rank` IS NOT NULL
  OR `previous_domcop_rank` IS NOT NULL
  OR `previous_majestic_rank` IS NOT NULL
  OR `previous_umbrella_rank` IS NOT NULL;


