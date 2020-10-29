-- This script help to identify why an object is missing from the end user share
-- Usage:
--   1. update variables database_name & table_name
--   2. execute all 'set' commands to set up the environment
--   3. run through the subsequent commands to find the problem

-- Input parameters
set database_name = upper('SPGLOBAL_EXPRESS_FEED');
set table_name = upper('ciqFinInstance_Flat');

-- set up the environment; run all 'set' commands below 
set source_information_schema_tables=$database_name||'."INFORMATION_SCHEMA"."TABLES"';
set local_information_schema_tables=$database_name||'_LOCAL."INFORMATION_SCHEMA"."TABLES"';
set shared_information_schema_tables=$database_name||'_SHARED."INFORMATION_SCHEMA"."TABLES"';
set share_name = $database_name||'_SHARED_SHARE';
set log = $database_name||'_LOCAL.SMART_SYNC_METADATA.LOG';
set object_log =  $database_name||'_LOCAL.SMART_SYNC_METADATA.OBJECT_LOG';


-- check whether table/view exists in shared source
select * from identifier($source_information_schema_tables) where upper(table_name) = $table_name;
-- if table/view doesn't exist, contact provider

-- check whether table/view has been copied/synced. Note: The table name/view name has a version appendix
select * from identifier($local_information_schema_tables) where upper(table_name) like $table_name||'%';
-- if table doesn't exist, run sp_smart_sync('SYNC',...)

-- check whether secure view has been created/refreshed.
select * from identifier($shared_information_schema_tables) where upper(table_name) = $table_name;
-- if table doesn't exist run sp_smart_sync('REFRESH',...)

-- check whether SELECT has been granted to share for secure view .
show grants to share identifier($share_name);
select * from table(result_scan(last_query_id())) where upper("name") like '%'||$table_name||'%';
-- if grant doesnt exist, call Robert :-)

-- Further troubleshooting
--when and by what session was table/view synced
select * from identifier($log) where lower(message) like '%'||$table_name||'%';

-- take session_id and find all statements in query_history
--show history for table in local database
select * from identifier($object_log) where upper(table_name) like $table_name||'%';