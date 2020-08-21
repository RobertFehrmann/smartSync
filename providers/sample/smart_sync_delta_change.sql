CREATE OR REPLACE VIEW <local database>.smart_sync_metadata.smart_sync_delta_change AS 
    SELECT  table_schema object_schema
            ,table_name object_name
            ,bytes
            ,hash(table_schema,table_name,last_altered) request_id
            ,convert_timezone('UTC',current_timestamp) request_ts
    FROM <shared database>.information_schema.tables
    WHERE table_schema != 'INFORMATION_SCHEMA'
    ORDER BY object_schema,object_name 
    ;