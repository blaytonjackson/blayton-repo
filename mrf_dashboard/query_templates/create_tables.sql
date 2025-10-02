CREATE TABLE datahouse.public.mrf_claims_dashboard_customer_names (customer_name VARCHAR);

CREATE TABLE datahouse.public.mrf_claims_dashboard_file_ingestion_metadata (
    id VARCHAR,
    file_path text,
    file_size VARCHAR,
    file_type VARCHAR,
    has_835 boolean,
    has_837 boolean,
    is_broken boolean,
    is_parsed boolean,
    is_processed boolean,
    is_removed boolean,
    is_ready_to_be_removed boolean,
    created_at timestamp,
    parsed_at timestamp,
    remove_at timestamp,
    parsing_process_id VARCHAR,
    error_message VARCHAR,
    has_errors boolean,
    error_type VARCHAR,
    customer_name VARCHAR
);

CREATE TABLE datahouse.public.mrf_claims_dashboard_835_837_stats (
    organization_id VARCHAR,
    name VARCHAR,
    npi VARCHAR,
    org_name VARCHAR,
    customer_name VARCHAR,
    npi_835 VARCHAR,
    min_date_835 TIMESTAMP,
    max_date_835 TIMESTAMP,
    records_835 DECIMAL(34, 2),
    npi_837 VARCHAR,
    min_date_837 TIMESTAMP,
    max_date_837 TIMESTAMP,
    records_837 DECIMAL(34, 2)
);