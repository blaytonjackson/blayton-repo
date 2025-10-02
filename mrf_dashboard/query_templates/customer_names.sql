SELECT DISTINCT table_schema as customer_database 
FROM information_schema.tables 
WHERE table_schema ILIKE '%customer%' 
ORDER BY table_schema ASC