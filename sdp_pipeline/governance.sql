-- governance.sql -- UC tags and comments for SDP pipeline tables
-- Run this AFTER the SDP pipeline has created all tables (bronze, silver, gold).
-- NOTE: Access grants (GRANT statements) are documented at the bottom as comments.
-- Run them separately in a SQL editor with appropriate permissions.

-- 1. TABLE-LEVEL TAGS
ALTER TABLE exmox.bronze.bronze_installs      SET TAGS ('layer'='bronze',  'domain'='installs',      'quality'='raw',        'retention'='90-days');
ALTER TABLE exmox.bronze.bronze_events       SET TAGS ('layer'='bronze',  'domain'='events',        'quality'='raw',        'retention'='90-days');
ALTER TABLE exmox.bronze.bronze_offers       SET TAGS ('layer'='bronze',  'domain'='offers',        'quality'='raw',        'retention'='90-days');
ALTER TABLE exmox.bronze.bronze_user_profile SET TAGS ('layer'='bronze',  'domain'='user_profile',  'quality'='raw',        'retention'='90-days');
ALTER TABLE exmox.silver.silver_installs      SET TAGS ('layer'='silver', 'domain'='installs',      'quality'='validated',   'retention'='1-year');
ALTER TABLE exmox.silver.silver_events        SET TAGS ('layer'='silver', 'domain'='events',        'quality'='validated',   'retention'='1-year');
ALTER TABLE exmox.silver.silver_offers        SET TAGS ('layer'='silver', 'domain'='offers',        'quality'='validated',   'retention'='1-year');
ALTER TABLE exmox.silver.silver_user_profile  SET TAGS ('layer'='silver', 'domain'='user_profile',  'quality'='validated',   'retention'='1-year');
ALTER TABLE exmox.silver.silver_rejects       SET TAGS ('layer'='silver', 'domain'='rejects',       'quality'='quarantined', 'retention'='1-year');
ALTER TABLE exmox.silver.silver_dq_metrics    SET TAGS ('layer'='silver', 'domain'='dq',            'quality'='metadata',    'retention'='1-year');
ALTER TABLE exmox.silver.silver_dq_issues     SET TAGS ('layer'='silver', 'domain'='dq',            'quality'='metadata',    'retention'='1-year');
ALTER TABLE exmox.gold.gold_daily_country_platform SET TAGS ('layer'='gold', 'domain'='aggregates', 'quality'='aggregated', 'retention'='2-years');

-- 2. COLUMN-LEVEL PII TAGS
ALTER TABLE exmox.bronze.bronze_installs      ALTER COLUMN user_id         SET TAGS ('pii'='true', 'pii_type'='user_identifier');
ALTER TABLE exmox.bronze.bronze_installs      ALTER COLUMN device_model    SET TAGS ('pii'='true', 'pii_type'='device_info');
ALTER TABLE exmox.bronze.bronze_events        ALTER COLUMN user_id         SET TAGS ('pii'='true', 'pii_type'='user_identifier');
ALTER TABLE exmox.bronze.bronze_events        ALTER COLUMN event_id        SET TAGS ('pii'='true', 'pii_type'='event_identifier');
ALTER TABLE exmox.bronze.bronze_user_profile  ALTER COLUMN user_id         SET TAGS ('pii'='true', 'pii_type'='user_identifier');
ALTER TABLE exmox.bronze.bronze_user_profile  ALTER COLUMN revenue_30d_eur SET TAGS ('pii'='true', 'pii_type'='financial_data');
ALTER TABLE exmox.silver.silver_installs      ALTER COLUMN user_id         SET TAGS ('pii'='true', 'pii_type'='user_identifier');
ALTER TABLE exmox.silver.silver_installs      ALTER COLUMN device_model    SET TAGS ('pii'='true', 'pii_type'='device_info');
ALTER TABLE exmox.silver.silver_events        ALTER COLUMN user_id         SET TAGS ('pii'='true', 'pii_type'='user_identifier');
ALTER TABLE exmox.silver.silver_events        ALTER COLUMN event_id        SET TAGS ('pii'='true', 'pii_type'='event_identifier');
ALTER TABLE exmox.silver.silver_user_profile  ALTER COLUMN user_id         SET TAGS ('pii'='true', 'pii_type'='user_identifier');
ALTER TABLE exmox.silver.silver_user_profile  ALTER COLUMN revenue_30d_eur SET TAGS ('pii'='true', 'pii_type'='financial_data');
ALTER TABLE exmox.silver.silver_rejects       ALTER COLUMN reject_key      SET TAGS ('pii'='true', 'pii_type'='identifier');
ALTER TABLE exmox.silver.silver_rejects       ALTER COLUMN raw_record      SET TAGS ('pii'='true', 'pii_type'='raw_pii_payload');

-- 3. TABLE COMMENTS
COMMENT ON TABLE exmox.bronze.bronze_installs      IS 'Raw install data from Auto Loader + S3 (CSV, all STRING). Key: user_id.';
COMMENT ON TABLE exmox.bronze.bronze_events       IS 'Raw event data from Auto Loader + S3 (CSV, all STRING). Key: event_id.';
COMMENT ON TABLE exmox.bronze.bronze_offers       IS 'Raw offer data from Auto Loader + S3 (CSV, all STRING). Key: offer_id.';
COMMENT ON TABLE exmox.bronze.bronze_user_profile  IS 'Raw user profile data from Auto Loader + S3 (CSV, all STRING). Key: user_id.';
COMMENT ON TABLE exmox.silver.silver_installs      IS 'Cleaned, deduplicated install data. Platform derived from device_model. DQ-validated via SDP expectations.';
COMMENT ON TABLE exmox.silver.silver_events        IS 'Cleaned, deduplicated event data. First-arrival wins. Enriched with orphan/pre-install/late flags. DQ-validated via SDP expectations.';
COMMENT ON TABLE exmox.silver.silver_offers        IS 'Cleaned, deduplicated offer data. Latest version wins. DQ-validated via SDP expectations.';
COMMENT ON TABLE exmox.silver.silver_user_profile  IS 'Cleaned, deduplicated user profile data. Latest version wins. DQ-validated via SDP expectations.';
COMMENT ON TABLE exmox.silver.silver_rejects       IS 'Rejected rows from all bronze sources with reject_reason and raw_record JSON. Computed as SDP materialized view.';
COMMENT ON TABLE exmox.silver.silver_dq_metrics    IS 'Data quality rule evaluation metrics. One row per rule per refresh.';
COMMENT ON TABLE exmox.silver.silver_dq_issues     IS 'Aggregated issue counts from silver_rejects. One row per issue type per refresh.';
COMMENT ON TABLE exmox.gold.gold_daily_country_platform IS 'Daily aggregates by country and platform. Installs, unique users per event type, reward payouts and cost in EUR. is_settled uses 6-day lookback.';

-- 4. ACCESS GRANTS -- run these separately in a SQL editor
-- Bronze (data_engineers only)
-- GRANT MODIFY ON TABLE exmox.bronze.bronze_installs      TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.bronze.bronze_installs      TO `data_engineers`;
-- GRANT MODIFY ON TABLE exmox.bronze.bronze_events       TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.bronze.bronze_events       TO `data_engineers`;
-- GRANT MODIFY ON TABLE exmox.bronze.bronze_offers       TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.bronze.bronze_offers       TO `data_engineers`;
-- GRANT MODIFY ON TABLE exmox.bronze.bronze_user_profile  TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.bronze.bronze_user_profile  TO `data_engineers`;
-- Silver (data_engineers + data_analysts)
-- GRANT MODIFY ON TABLE exmox.silver.silver_installs      TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_installs      TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_installs      TO `data_analysts`;
-- GRANT MODIFY ON TABLE exmox.silver.silver_events        TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_events        TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_events        TO `data_analysts`;
-- GRANT MODIFY ON TABLE exmox.silver.silver_offers        TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_offers        TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_offers        TO `data_analysts`;
-- GRANT MODIFY ON TABLE exmox.silver.silver_user_profile  TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_user_profile  TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_user_profile  TO `data_analysts`;
-- GRANT SELECT ON TABLE exmox.silver.silver_rejects       TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_dq_metrics    TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_dq_metrics    TO `data_quality`;
-- GRANT SELECT ON TABLE exmox.silver.silver_dq_issues     TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.silver.silver_dq_issues     TO `data_quality`;
-- Gold (all roles)
-- GRANT MODIFY ON TABLE exmox.gold.gold_daily_country_platform TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.gold.gold_daily_country_platform TO `data_engineers`;
-- GRANT SELECT ON TABLE exmox.gold.gold_daily_country_platform TO `data_analysts`;
-- GRANT SELECT ON TABLE exmox.gold.gold_daily_country_platform TO `data_quality`;