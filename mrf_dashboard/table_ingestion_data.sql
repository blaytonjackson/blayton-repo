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


COPY datahouse.public.mrf_claims_dashboard_file_ingestion_metadata
FROM 's3://turquoise-health-payer-export-main/bcj_claims_data_testing/file_ingestion'
IAM_ROLE 'arn:aws:iam::053696186450:role/RedshiftRole'
TRUNCATECOLUMNS
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;

SELECT
    userid,
    filename,
    line_number,
    raw_line,
    err_reason
FROM
    stl_load_errors
WHERE
    filename LIKE '%table_ingestion_data%'
ORDER BY
    starttime DESC
LIMIT 10;

SELECT * FROM datahouse.public.mrf_claims_dashboard_file_ingestion_metadata;