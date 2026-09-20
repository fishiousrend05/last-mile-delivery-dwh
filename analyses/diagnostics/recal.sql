SELECT 
    table_name,
    COUNT(column_name) as total_columns,
    STRING_AGG(column_name || ' (' || data_type || ')', ', ' ORDER BY ordinal_position) as schema_structure
FROM information_schema.columns
WHERE table_schema = 'marts'
GROUP BY table_name
ORDER BY table_name;