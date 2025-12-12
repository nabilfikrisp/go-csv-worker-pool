CREATE TABLE IF NOT EXISTS domain_ranking (
    global_rank INTEGER PRIMARY KEY,
    tld_rank INTEGER,
    domain TEXT,
    tld TEXT,
    ref_subnets INTEGER,
    ref_ips INTEGER,
    idn_domain TEXT,
    idn_tld TEXT,
    prev_global_rank INTEGER,
    prev_tld_rank INTEGER,
    prev_ref_subnets INTEGER,
    prev_ref_ips INTEGER
);

CREATE INDEX IF NOT EXISTS idx_domain ON domain_ranking (domain);

CREATE INDEX IF NOT EXISTS idx_tld ON domain_ranking (tld);