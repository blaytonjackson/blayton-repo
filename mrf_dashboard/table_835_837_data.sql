-- output of claims .py script
-- uploaded to S3 and then put into a table

CREATE TABLE datahouse.public.mrf_claims_dashboard_835_837_stats(
 	organization_id VARCHAR,
 	name VARCHAR,
 	npi VARCHAR,
 	org_name VARCHAR,
 	customer_name VARCHAR,
 	npi_835 VARCHAR,
 	min_date_835 TIMESTAMP,
 	max_date_835 TIMESTAMP,
 	records_835 DECIMAL(34,2),
 	npi_837 VARCHAR,
 	min_date_837 TIMESTAMP,
 	max_date_837 TIMESTAMP,
 	records_837 DECIMAL(34,2)
);

TRUNCATE TABLE datahouse.public.mrf_claims_dashboard_835_837_stats;

COPY datahouse.public.mrf_claims_dashboard_835_837_stats
FROM 's3://turquoise-health-payer-export-main/bcj_claims_data_testing/table_835_837_data'
IAM_ROLE 'arn:aws:iam::053696186450:role/RedshiftRole'
TRUNCATECOLUMNS
FORMAT AS CSV
DELIMITER ','
IGNOREHEADER 1;

SELECT * FROM datahouse.public.mrf_claims_dashboard_835_837_stats

SELECT
    userid,
    filename,
    line_number,
    raw_line,
    err_reason
FROM
    stl_load_errors
WHERE
    filename LIKE '%_835_837_%'
ORDER BY
    starttime DESC
LIMIT 10;
