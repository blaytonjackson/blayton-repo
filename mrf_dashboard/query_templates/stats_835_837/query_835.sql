WITH ranges_cte AS (
    SELECT *
    FROM (
            SELECT TO_CHAR(service_date, 'Mon YYYY') AS monthyear,
                COALESCE(provider_npi, payee_id_1000b) AS provider_npi,
                COUNT(*) AS total_records
            FROM claims.{{ customer_schema }}.imported_835s
            WHERE COALESCE(provider_npi, payee_id_1000b) IS NOT NULL
            GROUP BY TO_CHAR(service_date, 'Mon YYYY'),
                COALESCE(provider_npi, payee_id_1000b)
        ) aa
    WHERE total_records > 50
)
SELECT '{{ customer_schema }}' AS customer_name,
    COALESCE(b.provider_npi, payee_id_1000b) AS npi_835,
    MIN(service_date) AS min_date_835,
    MAX(service_date) AS max_date_835,
    COUNT(*) AS records_835
FROM claims.{{ customer_schema }}.imported_835s b
    JOIN ranges_cte c 
    ON COALESCE(b.provider_npi, payee_id_1000b) = c.provider_npi
    AND TO_CHAR(b.service_date, 'Mon YYYY') = c.monthyear
GROUP BY COALESCE(b.provider_npi, payee_id_1000b);