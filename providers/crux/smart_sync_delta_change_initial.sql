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
        FROM  (SELECT table_name
               FROM   <CRUX shared database name>.information_schema.tables 
               WHERE table_schema='CRUX_GENERAL') i
            INNER JOINT <fully qualified path to CRUX notifation table> n ON n.view_name = i.table_name
        WHERE crux_ingestion_dt < <delta_sync_start_date>)
    WHERE id = 1
    ORDER BY object_name;