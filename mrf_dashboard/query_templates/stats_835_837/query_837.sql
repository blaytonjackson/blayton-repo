WITH ranges_cte AS (
    SELECT *
    FROM (
            SELECT TO_CHAR(service_start_date, 'Mon YYYY') AS monthyear,
                billing_provider_id,
                COUNT(*) AS total_records
            FROM claims.{{ customer_schema }}.imported_837s
            WHERE billing_provider_id IS NOT NULL
            GROUP BY TO_CHAR(service_start_date, 'Mon YYYY'),
                billing_provider_id
        ) aa
    WHERE total_records > 50
)
SELECT '{{ customer_schema }}' AS customer_name,
    b.billing_provider_id AS npi_837,
    MIN(service_start_date) AS min_date_837,
    MAX(service_start_date) AS max_date_837,
    COUNT(*) AS records_837
FROM claims.{{ customer_schema }}.imported_837s b
    JOIN ranges_cte c ON b.billing_provider_id = c.billing_provider_id
    AND TO_CHAR(b.service_start_date, 'Mon YYYY') = c.monthyear
GROUP BY b.billing_provider_id;