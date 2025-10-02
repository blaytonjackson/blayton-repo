COPY datahouse.public.{{ table_name }} 
FROM 's3://turquoise-health-payer-export-main/bcj_claims_data_testing/{{ subdirectory_name }}' 
IAM_ROLE '{{ role }}' TRUNCATECOLUMNS 
FORMAT AS CSV DELIMITER ',' IGNOREHEADER 1;