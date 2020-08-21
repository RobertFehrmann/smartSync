CREATE OR REPLACE VIEW <local database>.smart_sync_metadata.smart_sync_delta_change AS 
    SELECT  'CRUX_GENERAL' object_schema
            ,object_name
            ,null::bigint bytes
            , hash(crux_resource_id, crux_delivery_version, crux_ingestion_dt, notification_dt ) request_id
            , convert_timezone('US/Eastern','UTC',crux_ingestion_dt::timestamp)::timestamp_tz(9) request_ts
    FROM (
        SELECT  view_name object_name
                ,crux_resource_id
                ,crux_delivery_version
                ,crux_ingestion_dt
                ,notification_dt
                , row_number() OVER (PARTITION BY view_name ORDER BY crux_ingestion_dt desc, notification_dt desc) id
        FROM  <fully qualified path to CRUX notifation table>)
    WHERE id = 1
    ORDER BY object_name;