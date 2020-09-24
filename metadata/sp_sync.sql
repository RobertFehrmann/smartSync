create or replace procedure METADATA.SP_SYNC(I_METHOD VARCHAR,I_METHOD_PARAM float, I_SRC_DB VARCHAR, I_TGT_DB VARCHAR)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     Smart Sync is a library of function to sync a remote (usually shared) Database
//             into a local database. This is useful in case a local database is required, for
//             instance for replication or sharing purposed.
//             The stored procedure can be called for the following methods:
//                SCHEDULER: The scheduler method is the root method of the sync process. It 
//                   determines the list of table to be syncd. If the number of tables to be 
//                   syncd is below the threshold of what one process is expected to manage
//                   it processes all those tables directly. In case there are more tables
//                   to be syncd it partitions the list into multiple partitions that have
//                   approx. the same amount of work. The amount of work is estimated by the
//                   total amount of data to be syncd and the fact that there is a fixed cost
//                   for each table. Therefor, partitions have different number of tables to sync.
//                   Then it schedules a task for each partition and waits for completions of each 
//                   task. After all tasks have completed successfully, it writes metadata information
//                   (current location of the new table (schema and name), size (bytes and row_count), 
//                   a hash aggregate (i.e reasonably unique fingerprint of the current content of
//                   the source object) and timing information into an object log table.
//                WORKER: The worker method performs the actual work of data synchornization. Tables are
//                   updated in place for each snapshot  a new copy will be created with an 
//                   incremental number added at the and of the table name. 
//                   The worker method finds it work by a partition ID that is passed in via the 
//                   CLUSTER_COUNT parameter. It collects meta data information like source object
//                   fingerprint. The fingerprint is used to determine if the source object has 
//                   changed. A new snapshot of the source object will only be create in case the 
//                   source object has changed. Then it collects metadata information from the target object.
//                COMPACTOR: Over time, the framework creates multiple copies of each table. These 
//                   copies provide a complete history of all snapshots. The compact method removes no
//                   longer needed copies.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------
// Modification History
//
// 2020-08-01 Robert Fehrmann  
//      Initial Version
// -----------------------------------------------------------------------------

// copy parameters into local constant; none of the values can be modified
const method= I_METHOD;
const cluster_count=I_METHOD_PARAM;
const worker_id=I_METHOD_PARAM;
const copies_to_keep=I_METHOD_PARAM;
const requested_refresh_id=I_METHOD_PARAM
const src_db  = I_SRC_DB;
const tgt_db  = I_TGT_DB;

// all dates in smart sync are in UTC. Change the timezone in case you want 
// to represent dates in a different timezone
const timezone='UTC';
//const timezone='US/Eastern';
// change max_copies if you want to keep more than 14 sync copies
// you can trim the copies via the COMPACT method as well
const max_copies=14;

// smart sync creates tasks which reference the this stored procedure. 
// Change these constants in case you want to host the stored procedure 
// location
const smart_sync_db="SMART_SYNC_DB";
const smart_sync_meta_schema="METADATA";

// constants for internal objects
const internal = "SMART_SYNC";
const meta_schema = internal + "_METADATA";
const scheduler_tmp = internal + "_SCHEDULER_TMP";
const refresh_tmp = internal + "_REFRESH_TMP";
const task_partitioned = "TASK_PARTITIONED";
const task_partitioned_set = "TASK_PARTITIONED_SET"
const object_sync_request = "OBJECT_SYNC_REQUEST";
const object_sync_task = "OBJECT_SYNC_TASK";
const object_sync_result = "OBJECT_SYNC_RESULT";
const metadata_out = "METADATA_OUT";
const object_drop_request = "OBJECT_DROP_REQUEST";
const smart_sync_delta_change = "SMART_SYNC_DELTA_CHANGE";
const object_snapshot_meta_data = "OBJECT_SNAPSHOT_META_DATA";
const task_name_worker_sync = tgt_db + "_WORKER_SYNC";
const task_name_worker_refresh = tgt_db + "_WORKER_REFRESH";
const local_object_log = "OBJECT_LOG";
const local_log="LOG"
const remote="REMOTE"
const remote_object_log = remote+"_OBJECT_LOG";
const remote_log=remote+"_LOG"

const table_scheduler = "SCHEDULER";
const tgt_share = tgt_db + "_SHARE"

// internal limit
const max_number_schemas=365;
const worker_timeout=3*60*60*1000;
const wait_to_poll=30;
const max_worker_wait_loop = worker_timeout/60/1000/(wait_to_poll/60);
const min_jobs_per_cluster = 16;
const task_partition_set_size=16;
const max_partition=32;
const refresh_cluster_count=24;
const refresh_min_jobs_per_cluster=64;

// internal enum constants
const method_sync='SYNC';
const method_worker_sync='WORKER_SYNC';
const method_compact='COMPACT';
const method_refresh='REFRESH'
const method_worker_refresh='WORKER_REFRESH';
const action_create='CREATE';
const action_reference='REFERENCE';
const action_drop='DROP';
const string_yes='YES';
const string_no ='NO';

const status_begin = "BEGIN";
const status_end = "END";
const status_wait = "WAIT TO COMPLETE";
const status_warning = "WARNING";
const status_failure = "FAILURE";
const version_default = "000000";
const version_initial = "000001";

// internal variables
var return_array = [];
var counter = 0;
var loop_counter = 0;
var prev_schema="PREV";
var status= status_end;
var scheduler_session_id=0;
var worker_session_id=0;
var partition_id=0;
var current_warehouse=""
var current_timestamp;


var this_name = Object.keys(this)[0];
var procName = this_name + "-" + method;


// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     The log function is used to store log messages locally in an array
//             The array can be persistet in a log table by calling function 
//             flush log.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function log ( msg ) {
   var d=new Date();
   var UTCTimeString=("00"+d.getUTCHours()).slice(-2)+":"+("00"+d.getUTCMinutes()).slice(-2)+":"+("00"+d.getUTCSeconds()).slice(-2);
   return_array.push(UTCTimeString+" "+msg);
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     The flush_log function is used to store the locally stored log 
//             messages in a database table.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function flush_log (status){
   var message="";
   var sqlquery="";
   for (i=0; i < return_array.length; i++) {
      message=message+String.fromCharCode(13)+return_array[i];
   }
   message=message.replace(/'/g,""); //' keep formatting in VS nice

   for (i=0; i<2; i++) {
      try {

         var sqlquery = "INSERT INTO \"" + tgt_db + "\"." + meta_schema + "."+local_log+" ( scheduler_session_id, partition_id, method, status,message) values ";
         sqlquery = sqlquery + "("+scheduler_session_id +","+partition_id+ ",'" + method + "','" + status + "','" + message + "');";
         snowflake.execute({sqlText: sqlquery});
         break;
      }
      catch (err) {
         sqlquery=`
            CREATE TABLE IF NOT EXISTS "`+ tgt_db + `".` + meta_schema + `.`+local_log+` (
               id integer AUTOINCREMENT (0,1)
               ,create_ts timestamp_tz(9) default convert_timezone('`+timezone+`',current_timestamp)
               ,scheduler_session_id number default 0
               ,session_id number default to_number(current_session())
               ,partition_id integer
               ,method varchar
               ,status varchar
               ,message varchar)`;
         snowflake.execute({sqlText: sqlquery});
      }
   }
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function gets metadata information from the metadata schema 
//             from the target database. It is necessary because Snowflake currently
//             only supports result sets from the metadata tables of up to 10k rows. 
//             This method incrementally stored the result in a local table.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function get_metadata(metadata_db){

   var sqlquery="";
   var schema_name="";
   var counter=0;

   log("GET METADATA FOR "+metadata_db);

   sqlquery=`
      CREATE OR REPLACE
      TABLE "` + tgt_db + `"."` + scheduler_tmp + `"."` + metadata_out  + `" (
         table_type varchar
         ,table_catalog varchar
         ,table_schema varchar
         ,table_name varchar
         ,bytes number
         ,row_count number
         ,last_altered timestamp_tz(9)
      )`;
   snowflake.execute({sqlText: sqlquery});

   try {
      sqlquery=`
         INSERT 
         INTO "` + tgt_db + `"."` + scheduler_tmp + `"."` + metadata_out  + `"
            SELECT table_type, table_catalog, table_schema, table_name 
                   ,bytes, row_count, last_altered
            FROM  "`+metadata_db+`".information_schema.tables 
            WHERE table_catalog='`+metadata_db+`'
            `;
      snowflake.execute({sqlText:  sqlquery});

   }
   catch(err) {
      sqlquery=`
         SELECT schema_name
         FROM "`+metadata_db+`".information_schema.schemata 
      `;

      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

      while (ResultSet.next() && counter < max_number_schemas ) {
         counter+=1;
         schema_name=ResultSet.getColumnValue(1);
         log("   GET METADATA FOR: "+schema_name)

         sqlquery=`
            INSERT 
            INTO "` + tgt_db + `"."` + scheduler_tmp + `"."` + metadata_out  + `"
               SELECT table_type, table_catalog, table_schema, table_name 
                     ,bytes, row_count, last_altered
               FROM "`+metadata_db+`".information_schema.tables 
               WHERE table_catalog='`+metadata_db+`' AND  table_schema ='`+schema_name+`'
               `;
         snowflake.execute({sqlText:  sqlquery});
      }
   }
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function is the main function to process the work for the worker process
//             Instead of running the metadata collection individually per object, it collects 
//             the metadata information in sets to increase performance. Then it determines 
//             what objects actually have to be copied by filtering objects that have the same
//             fingerprint as the source object. All remaining objects will be copied into a 
//             new snapshot table using a CTAS statement. Then it collects the metadata information
//             for the new target objects using the same method as above.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function process_sync_tasks(partition_id) {
   var counter=0;
   var prev_schema="";
   var table_name="";
   var table_schema="";
   var delivery_id="";
   var curr_schema_name="";
   var curr_table_version="";
   var next_schema_name="";
   var next_table_version="";
   var sqlquery="";

   log("GET TASK LIST")

   // split the work into sets of "task_partition_size" chunks
   sqlquery=`
      INSERT INTO "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned_set + ` 
         SELECT partition_id, trunc(seq4()/`+task_partition_set_size+`) set_id,check_fingerprint,'`+src_db+`' database_name
                , object_schema, object_name
                , null::varchar, null::varchar, null varchar
         FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned + `
         WHERE partition_id = `+partition_id+` 
         ORDER BY object_schema, object_name 
   `;
   snowflake.execute({sqlText:  sqlquery});

   // iterate over all sets and collect metadata, i.e. a fingerprint for each source object
   sqlquery=`
      SELECT set_id,object_schema
      FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned_set + `
      WHERE partition_id = `+partition_id+`
      GROUP BY set_id,object_schema
      ORDER BY set_id,object_schema
   `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   while(ResultSet.next()) {
      var set_id=ResultSet.getColumnValue(1);
      var object_schema=ResultSet.getColumnValue(2)

      // the call to information_schema in the following statement may fail while Snowflake still has a limitation
      // on how many rows this call can return. While this is still the case, we will take attributes row_count, bytes
      // from table task_partitioned. The drawback from this approach is, that the fingerprint is no longer computes in
      // the same transaction as row_count, bytes. In case the table content has changed since the row_count, bytes have 
      // been computed, the fingerprint would show a different value, while row_count, bytes still show the old value
      // (which might be confusing).     

      sqlquery=`
         SELECT 'INSERT INTO "` + tgt_db + `".` + scheduler_tmp + `.` + object_snapshot_meta_data + ` 
            SELECT s.partition_id, s.database_name, s.table_schema, s.table_name, s.fingerprint'||
                   ',convert_timezone(\\'`+timezone+`\\',i.last_altered) last_altered
                   , i.bytes, i.row_count FROM (\n'|| listagg(stmt,' union \n')||'\n) s ` +
                   // INNER JOIN "`+src_db+`".information_schema.tables  i '||
                   //' on i.table_catalog=\\'`+src_db+`\\' and i.table_schema=s.table_schema and i.table_name = s.table_name ' 
                   ` INNER JOIN "`+tgt_db+`".` + scheduler_tmp + `.` + task_partitioned + `  i '||
                   ' on i.object_schema=s.table_schema and i.object_name = s.table_name ' 
            FROM   (
               SELECT '( SELECT `+partition_id+` partition_id,\\'`+src_db+`\\' database_name'||
                              ', \\''||object_schema||'\\' table_schema,\\''||object_name||'\\' table_name'||
                              ','||iff(check_fingerprint='`+string_yes+`','(SELECT hash_agg(*) FROM "`+src_db+`"."'||
                                             object_schema||'"."'||object_name||'")','null')||'::bigint fingerprint '||
                        ')' stmt
               FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned_set + ` 
               WHERE partition_id = `+partition_id+` AND set_id = `+set_id+` 
                  AND object_schema = '`+object_schema+`' and database_name = '`+src_db+`'
               ORDER BY object_schema, object_name ) 
            ;`

      var ResultSet2 = (snowflake.createStatement({sqlText:sqlquery})).execute();
   
      if (ResultSet2.next()) {
         sqlquery='';
         sqlquery=ResultSet2.getColumnValue(1)
         snowflake.execute({sqlText: sqlquery});
      } else {
         throw new Error('TASK LIST NOT FOUND');
      }
   }

   // filter all objects with fingerprints that have already been copied and create a new
   // snapshot table for all remaining objects
   sqlquery=`
      WITH fingerprint_log AS
         (
            SELECT table_schema, table_name, target_fingerprint
            FROM (
               SELECT table_schema, table_name, target_fingerprint
                     ,row_number() OVER (PARTITION BY table_schema, table_name ORDER BY curr_table_version desc) id
               FROM "` + tgt_db + `".` + meta_schema + `.` + local_object_log + `
            )
            WHERE id=1 
         )
      SELECT tp.object_schema, tp.object_name 
             ,tp.curr_schema_name, tp.curr_table_version
             ,tp.next_schema_name, tp.next_table_version
      FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned + ` tp
      INNER JOIN "` + tgt_db + `".` + scheduler_tmp + `.` + object_snapshot_meta_data + ` s
               ON s.partition_id = tp.partition_id AND s.object_schema=tp.object_schema AND s.object_name=tp.object_name
      LEFT OUTER JOIN fingerprint_log fl
               ON fl.table_schema=s.object_schema AND fl.table_name=s.object_name AND nvl(fl.target_fingerprint,0)=nvl(s.fingerprint,1)
      WHERE tp.partition_id = `+partition_id+` AND fl.target_fingerprint is null 
      ORDER BY tp.bytes desc, tp.object_schema, tp.object_name
   `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   counter=0;
   while (ResultSet.next()) {
      counter+=1;
      table_schema=ResultSet.getColumnValue(1);
      table_name=ResultSet.getColumnValue(2);
      curr_schema_name=ResultSet.getColumnValue(3);
      curr_table_version=ResultSet.getColumnValue(4);
      next_schema_name=ResultSet.getColumnValue(5);
      next_table_version=ResultSet.getColumnValue(6);

      if (next_schema_name != prev_schema) {
         log("  CREATE SCHEMA: "+next_schema_name);
         prev_schema=next_schema_name;
         sqlquery=`
               CREATE SCHEMA IF NOT EXISTS "` + tgt_db + `"."` + next_schema_name + `"
         `;
         snowflake.execute({sqlText:  sqlquery});
      }

      log("     COPY "+table_schema+"."+table_name+" TO "+next_schema_name + "." + table_name + "_" + next_table_version);

      sqlquery=`
         CREATE /* # ` + counter + ` */ OR REPLACE
         TABLE "` + tgt_db + `"."` + next_schema_name + `"."` + table_name + '_' + next_table_version + `" AS
                     SELECT * FROM "` + src_db + `"."` + table_schema + `"."` + table_name + `"
      `;
      snowflake.execute({sqlText:  sqlquery});
   }

   // collect metadata information for all target objects in case at least 1 new table has been created. 
   if (counter>0) {

      // split the list of target tables into equal sets and collect the metadata, i.e. fingerprint for the 
      // snapshot table
      sqlquery=`
         INSERT INTO "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned_set + ` 
            WITH fingerprint_log AS 
               (
                  SELECT table_schema, table_name, target_fingerprint
                  FROM (
                     SELECT table_schema, table_name, target_fingerprint
                           ,row_number() OVER (PARTITION BY table_schema, table_name ORDER BY curr_table_version desc) id
                     FROM "` + tgt_db + `".` + meta_schema + `.` + local_object_log + `
                  )
                  WHERE id=1 
               )
             SELECT tp.partition_id, trunc(seq4()/`+task_partition_set_size+`) set_id,tp.check_fingerprint,'`+tgt_db+`' database_name
                  , tp.object_schema, tp.object_name, next_schema_name, next_table_version
                  ,iff(fl.target_fingerprint is null ,'YES','NO') perform_sync
            FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned + ` tp
            INNER JOIN "` + tgt_db + `".` + scheduler_tmp + `.` + object_snapshot_meta_data + ` s
                     ON s.partition_id = tp.partition_id AND s.object_schema=tp.object_schema AND s.object_name=tp.object_name
            LEFT OUTER JOIN fingerprint_log fl
                     ON fl.table_schema=s.object_schema AND fl.table_name=s.object_name AND nvl(fl.target_fingerprint,0)=nvl(s.fingerprint,1)
            WHERE tp.partition_id = `+partition_id+` AND fl.target_fingerprint is null 
      `;
      snowflake.execute({sqlText:  sqlquery});

      sqlquery=`
         SELECT distinct set_id
         FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned_set + `
         WHERE partition_id = `+partition_id+`
            AND database_name = '`+tgt_db+`'
         ORDER BY set_id
      `;

      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

      while(ResultSet.next()) {
         var set_id=ResultSet.getColumnValue(1);

         sqlquery=`
            SELECT 'INSERT INTO "` + tgt_db + `".` + scheduler_tmp + `.` + object_snapshot_meta_data + ` 
               SELECT partition_id, database_name, s.table_schema, s.table_name, fingerprint, null, null,null FROM (\n'||
                      listagg(stmt,' union \n')||'\n) s' 
               FROM   (
                  SELECT '( SELECT `+partition_id+` partition_id,\\'`+tgt_db+`\\' database_name'||
                                 ',\\''||object_schema||'\\' table_schema,\\''||object_name||'\\' table_name'||
                                 ','||iff(check_fingerprint='`+string_yes+`','(SELECT hash_agg(*) FROM "`+tgt_db+`"."'||next_schema_name||'"."'||
                                                                      object_name||'_'||next_table_version||'")','null')||'::bigint fingerprint '||
                           ')' stmt
               FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned_set + ` 
               WHERE partition_id = `+partition_id+` AND set_id = `+set_id+` AND database_name = '`+tgt_db+`'
               ORDER BY object_schema, object_name )
               ;`

         var ResultSet2 = (snowflake.createStatement({sqlText:sqlquery})).execute();

         if (ResultSet2.next()) {
            sqlquery='';
            sqlquery=ResultSet2.getColumnValue(1)
            snowflake.execute({sqlText: sqlquery});
         } else {
            throw new Error('TASK LIST NOT FOUND');
         }
      }
   }
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function is the main function to process the work for the worker process
//             Instead of running the metadata collection individually per object, it collects 
//             the metadata information in sets to increase performance. Then it determines 
//             what objects actually have to be copied by filtering objects that have the same
//             fingerprint as the source object. All remaining objects will be copied into a 
//             new snapshot table using a CTAS statement. Then it collects the metadata information
//             for the new target objects using the same method as above.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function process_refresh_tasks(partition_id) {
   var counter=0;
   var prev_schema="";
   var table_name="";
   var table_schema="";
   var delivery_id="";
   var curr_schema_name="";
   var curr_table_version="";
   var next_schema_name="";
   var next_table_version="";
   var sqlquery="";

   log("GET TASK LIST")

   sqlquery=`
      SELECT object_schema table_schema,object_name table_name,curr_schema_name, curr_table_name
      FROM  "` + tgt_db + `".` + refresh_tmp + `.` + task_partitioned + `
      WHERE partition_id = `+partition_id+`
      ORDER BY table_schema,table_name
   `;
   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   counter=0;
   while (ResultSet.next()) {
      counter = counter + 1;
      table_schema=ResultSet.getColumnValue(1);
      table_name = ResultSet.getColumnValue(2);
      curr_schema_name = ResultSet.getColumnValue(3);
      curr_table_name = ResultSet.getColumnValue(4);
      table_schema_new=internal+"_"+table_schema

      if (table_schema_new != prev_schema) {
         log("CREATE SCHEMA: "+table_schema_new);
         prev_schema=table_schema_new;

         sqlquery=`
            CREATE SCHEMA IF NOT EXISTS "` + tgt_db + `"."` + table_schema + `"
         `;
         snowflake.execute({sqlText:  sqlquery});

         sqlquery = `
            GRANT USAGE ON SCHEMA "` + tgt_db + `"."` + table_schema + `" TO SHARE "` + tgt_share + `"`;
         snowflake.execute({sqlText: sqlquery});

         sqlquery=`
            CREATE SCHEMA IF NOT EXISTS "` + tgt_db + `"."` + table_schema_new + `"
         `;
         snowflake.execute({sqlText:  sqlquery});

         sqlquery = `
            GRANT USAGE ON SCHEMA "` + tgt_db + `"."` + table_schema_new + `" TO SHARE "` + tgt_share + `"`;
         snowflake.execute({sqlText: sqlquery});
      }

      log("  CREATE SECURE VIEW: "+table_schema_new+"."+table_name+" FOR "+curr_schema_name+"."+curr_table_name);

      sqlquery = `
         CREATE OR REPLACE /* # ` + counter + ` */ SECURE VIEW "` + tgt_db + `"."` + table_schema_new + `"."` + table_name + `" AS
               SELECT * FROM "` + src_db + `"."` + curr_schema_name + `"."` + curr_table_name + `"`;
      snowflake.execute({sqlText: sqlquery});

      sqlquery = `
         GRANT SELECT ON /* # ` + counter + ` */ VIEW "` + tgt_db + `"."` + table_schema_new + `"."` + table_name + `" TO SHARE "` + tgt_share + `"`;
      snowflake.execute({sqlText: sqlquery});
   }

   if (counter==0){
      throw new Error("REQUESTED RUN ID NOT FOUND: "+requested_run_id)
   }

}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function records metadata for all new tables in an object log.
//             Recorded metadata is tracked by the original object_schema and object_name.
//             Metadata includes the location and names for the previous and current snapshots,
//             the fingerprints, last update timestamps, number of rows and number of bytes 
//             of the source and target objects. It also creates a reference records for all 
//             objects that have not been copied. (reference='YES')
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function record_work() {
   var objects_processed=0;

   log("RECORD WORK")

   get_metadata(tgt_db);

   sqlquery=`
      CREATE OR REPLACE TABLE  "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_sync_result  + `" 
         AS 
            SELECT o.object_schema,o.object_name
                  ,t.bytes, t.row_count, convert_timezone('`+timezone+`',t.last_altered) last_commit_ts
            FROM "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_task + ` o
            INNER JOIN "` + tgt_db + `".` + scheduler_tmp + `.` + metadata_out + ` t
               ON t.table_catalog='`+tgt_db+`' AND t.table_schema=o.next_schema_name 
                  AND t.table_name=o.object_name||'_'||o.next_table_version
         `;
   snowflake.execute({sqlText:  sqlquery});

   sqlquery=`
      SELECT COUNT(*)
      FROM  "` + tgt_db + `".` + scheduler_tmp + `.` + object_snapshot_meta_data  + `
      WHERE database_name='`+tgt_db+`'   
   `;
    
   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   if (ResultSet.next()) {
      objects_processed=ResultSet.getColumnValue(1); 
      log("OBJECTS COPIED: "+objects_processed);

      if (objects_processed>0) {

         sqlquery=`
            INSERT INTO "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log + `"
               SELECT date_part(epoch_milliseconds,convert_timezone('`+timezone+`',current_timestamp())) run_id
                     ,request_id, request_ts,table_schema,table_name
                     ,prev_schema_name,prev_table_version,curr_schema_name,curr_table_version
                     ,source_fingerprint,source_commit_ts,source_bytes,source_row_count
                     ,target_fingerprint,target_commit_ts,target_bytes,target_row_count
                     ,action
                     ,convert_timezone('`+timezone+`',current_timestamp()) create_ts 
                     ,fingerprint_check
               FROM ( 
                  SELECT *,row_number() OVER (PARTITION BY table_schema, table_name ORDER BY curr_table_version desc) id
                  FROM ((
                        SELECT s.request_id, s.request_ts
                           ,s.object_schema table_schema, s.object_name table_name
                           ,s.curr_schema_name prev_schema_name, s.curr_table_version prev_table_version 
                           ,s.next_schema_name curr_schema_name, s.next_table_version curr_table_version
                           ,ss.fingerprint source_fingerprint, ss.last_commit_ts source_commit_ts, ss.bytes source_bytes, ss.row_count source_row_count
                           ,st.fingerprint target_fingerprint, r.last_commit_ts target_commit_ts, r.bytes target_bytes, r.row_count target_row_count
                           ,'`+action_create+`' action
                           ,tp.perform_sync fingerprint_check
                        FROM   "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_sync_task + `" s
                        INNER JOIN "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_snapshot_meta_data  + `" ss
                              ON ss.object_schema=s.object_schema and ss.object_name=s.object_name and ss.database_name='`+src_db+`'             
                        INNER JOIN "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_snapshot_meta_data  + `" st
                              ON st.object_schema=s.object_schema and st.object_name=s.object_name and st.database_name='`+tgt_db+`'             
                        INNER JOIN "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_sync_result  + `" r
                              ON r.object_schema=s.object_schema and r.object_name=s.object_name
                        INNER JOIN "` + tgt_db + `"."` + scheduler_tmp + `"."` + task_partitioned_set  + `" tp
                              ON tp.object_schema=s.object_schema and tp.object_name=s.object_name and tp.database_name='`+tgt_db+`'
                     ) UNION (
                        SELECT request_id, request_ts
                           ,table_schema,table_name
                           ,prev_schema_name,prev_table_version 
                           ,curr_schema_name,curr_table_version
                           ,source_fingerprint,source_commit_ts,source_bytes,source_row_count
                           ,target_fingerprint,target_commit_ts,target_bytes,target_row_count
                           ,'`+action_reference+`' action
                           ,fingerprint_check
                        FROM "` + tgt_db + `".` + meta_schema + `.` + local_object_log + ` l1
                     )
                  )
               )
               WHERE id=1
               ORDER BY table_schema, table_name
         `;
         snowflake.execute({sqlText:  sqlquery});
      }
   } else {
      throw new Error('TASK TABLE NOT FOUND!');
   }
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function determines what needs to be done. The list of objects
//             to be syncd is either coming from the source database information schema
//             or it can be provides via a view called SMART_SYNC_DELTA_CHANGE. If the 
//             view does not exist it is assumed that all objects have to be syncd, i.e.
//             it is no necessary to collect fingerprint information. The total list
//             is then filtered by the last_altered data of the source objects. Only objects
//             that have been altered since the last run will be syncd. The resulting list
//             will be partitioned into groups of equal work.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function create_sync_task_list() {
   var sqlquery="";

   sqlquery=`
      SELECT table_name
      FROM  "` + tgt_db + `".information_schema.tables
      WHERE table_catalog='`+tgt_db+`' AND table_schema = '`+meta_schema+`' 
         AND TABLE_NAME = '`+smart_sync_delta_change+`'
      `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
   if (ResultSet.next()) {
      log("USING DELTA CHANGE TABLE: "+smart_sync_delta_change);

      sqlquery=`
         CREATE OR REPLACE TABLE "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_request + `
            AS 
               WITH most_recent_sync_object 
                  AS (
                     SELECT max(source_commit_ts) last_altered
                     FROM "` + tgt_db + `".` + meta_schema + `.` + local_object_log +`
                  )
               SELECT null::varchar object_type, '`+string_no+`'::varchar check_fingerprint, c.object_schema, c.object_name
                     , c.bytes, c.request_id, c.request_ts
               FROM  "` + tgt_db + `".` + meta_schema + `.` + smart_sync_delta_change +` c
                     , most_recent_sync_object 
               WHERE request_ts > nvl(last_altered,'2000-01-01'::timestamp_tz)
                  AND request_id NOT IN  (
                     SELECT request_id
                     FROM "` + tgt_db + `".` + meta_schema + `.` + local_object_log + ` l 
                  )
      `;

      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   } else {
      log("USING INFORMATION SCHEMA DB: "+src_db);

      sqlquery=`
         CREATE OR REPLACE TABLE "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_request + `
            AS 
               SELECT table_type object_type, '`+string_yes+`'::varchar check_fingerprint, table_schema object_schema, table_name object_name, bytes
                     , convert_timezone('`+timezone+`',current_timestamp) request_ts
                     , hash(table_schema,table_name,convert_timezone('`+timezone+`',last_altered)) request_id
               FROM "` + src_db + `".INFORMATION_SCHEMA.TABLES
               WHERE table_catalog = '`+src_db+`' AND table_schema != 'INFORMATION_SCHEMA' 
      `;
      snowflake.execute({sqlText: sqlquery});
   }

   log("CREATE TASK FOR REQUESTED OBJECTS");

   get_metadata(src_db);

   sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_task + `
         AS WITH
              source_objects 
                  AS (
                     SELECT table_type object_type, table_schema object_schema, table_name object_name, bytes
                            , row_count,last_altered
                     FROM "` + tgt_db + `".` + scheduler_tmp + `.` + metadata_out + ` t
                     WHERE table_catalog='`+src_db+`' AND table_schema != 'INFORMATION_SCHEMA' 
                  ),
               target_objects
                  AS (
                     SELECT table_schema object_schema,table_name object_name,target_bytes bytes, target_row_count row_count
                              ,prev_schema_name, prev_table_version, curr_schema_name, curr_table_version
                     FROM ( 
                           SELECT table_schema,table_name,target_bytes, target_row_count
                                    ,prev_schema_name, prev_table_version, curr_schema_name, curr_table_version
                                    ,row_number() OVER (PARTITION BY table_schema, table_name ORDER BY curr_table_version desc) id
                           FROM "` + tgt_db + `".` + meta_schema + `.` + local_object_log + ` 
                     )
                     WHERE id=1
                  )
               SELECT s.object_type, r.check_fingerprint, r.object_schema, r.object_name, curr_schema_name
                        , curr_table_version, r.object_schema||'_'|| to_varchar(request_ts,'YYYY_MM_DD') next_schema_name
                        , lpad((nvl(curr_table_version,'000000')::int+1),6,'0')::varchar next_table_version
                        , s.row_count row_count
                        , s.last_altered last_altered
                        , nvl(r.bytes,t.bytes) bytes
                        , r.request_id, r.request_ts
               FROM "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_request + ` r
                  INNER JOIN source_objects s ON s.object_schema=r.object_schema AND r.object_name=s.object_name
                  LEFT OUTER JOIN target_objects t ON t.object_schema=r.object_schema AND t.object_name=r.object_name
      `;
   snowflake.execute({sqlText: sqlquery});

   log("PARTITION TASK LIST ");

   sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `"."` + scheduler_tmp + `".` + task_partitioned + ` AS
          SELECT trunc(((SUM(((nvl(bytes,0)/1000000000)*12)::float) OVER (ORDER BY bytes,object_schema,object_name RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) +
                        (row_number() OVER (ORDER BY bytes,object_schema,object_name RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))) /
                       ((SELECT (sum(nvl(bytes,0)/1000000000)*12+1)::float FROM "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_task + `) +
                        (SELECT sum(1) FROM "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_task + `))
                     ) * least(
                           ceil((SELECT SUM(1) FROM "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_task + ` 
                                 )/`+min_jobs_per_cluster+`)
                              ,`+cluster_count+`))+1 partition_id, t.*
         FROM "` + tgt_db + `".` + scheduler_tmp + `.` + object_sync_task + ` t
         ORDER BY partition_id, object_schema, object_type, object_name
   `;

   snowflake.execute({sqlText: sqlquery});

}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function deletes all worker tasks in case of a fatal error or 
//             at the successful completion of all worker tasks.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function drop_all_workers (process) {

   var process_tmp="";
   var task_name_worker="";

   if (process==method_sync){
      process_tmp=scheduler_tmp;
      task_name_worker=task_name_worker_sync;
   } else if (process==method_refresh) {
      process_tmp=refresh_tmp;
      task_name_worker=task_name_worker_refresh;
   } else {
      throw new Error ("METHOD NOT FOUND: "+process)
   }

   log("DROP ALL WORKERS");
   var sqlquery=`
      SELECT partition_id
      FROM   "` + tgt_db + `".` + process_tmp + `.` + table_scheduler+ ` 
      ORDER BY 1
   `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   while (ResultSet.next()) {
      var partition_id_tmp=ResultSet.getColumnValue(1); 
      sqlquery=`
         DROP TASK "` + tgt_db + `".` + process_tmp + `."` + task_name_worker + `_` + partition_id_tmp + `"
      `;
      snowflake.execute({sqlText:  sqlquery});
   }

   //select SYSTEM$ABORT_SESSION( );

}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function schedules the worker tasks and then waits for their start 
//             and completion. It first creates a scheduling record for each task which. These 
//             These scheduler tasks and logging information from the worker tasks will be 
//             used for synchornization between the tasks.
//             Then we create one worker task per partition. The scheduler task (this task)
//             then waits till all worker tasks have completed (success or failure) by
//             checking the log table for the appropriate messages and sleeping in between.
//             After successful completion of all workers, it records the work in the object
//             log table.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function wait_for_worker_completion(process) {

   var process_tmp="";
   var task_name_worker="";
   var method_worker="";

   if (process==method_sync){
      process_tmp=scheduler_tmp;
      task_name_worker=task_name_worker_sync;
      method_worker=method_worker_sync;
   } else if (process==method_refresh){
      process_tmp=refresh_tmp
      task_name_worker=task_name_worker_refresh;
      method_worker=method_worker_refresh;
   } else {
      throw new error ("PROCESS NOT FOUND: "+process)
   }

   sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `"."` + process_tmp + `"."` + table_scheduler+ `" (
         scheduler_session_id bigint
         ,partition_id integer
         ,task_count integer
         ,create_ts timestamp_tz(9) default convert_timezone('`+timezone+`',current_timestamp)
      )
   `;
   snowflake.execute({sqlText:  sqlquery});

   sqlquery=`
      INSERT INTO "` + tgt_db + `"."` + process_tmp + `"."` + table_scheduler+ `" 
                  (scheduler_session_id,partition_id,task_count)
         SELECT current_session(),partition_id,count(1)
         FROM "` + tgt_db + `"."` + process_tmp + `".` + task_partitioned + `
         GROUP BY 1,2
   `;
   snowflake.execute({sqlText:  sqlquery});

   // create one task per configured number of clusters
   sqlquery=`
      SELECT partition_id
      FROM   "` + tgt_db + `"."` + process_tmp + `"."` + table_scheduler+ `" 
      ORDER BY 1
   `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   while (ResultSet.next()) {
      var partition_id_tmp=ResultSet.getColumnValue(1); 
      sqlquery=`
         CREATE OR REPLACE TASK "` + tgt_db + `".` + process_tmp + `."` + task_name_worker + `_` + partition_id_tmp +`"
            WAREHOUSE =  `+current_warehouse+`
            SCHEDULE= '1 MINUTE' 
            USER_TASK_TIMEOUT_MS=`+worker_timeout+` 
         AS call "`+smart_sync_db+`".`+smart_sync_meta_schema+`.`+this_name+`('`+method_worker+`',`+partition_id_tmp+`,'`+src_db+`','`+tgt_db+`')
      `;
      snowflake.execute({sqlText:  sqlquery});
      
      sqlquery=`
         ALTER TASK "` + tgt_db + `".` + process_tmp + `."` + task_name_worker + `_` + partition_id_tmp + `" resume
      `;
      snowflake.execute({sqlText:  sqlquery});
   }

   sqlquery=`
      WITH worker_log AS (
         SELECT partition_id, status, scheduler_session_id, session_id, create_ts
         FROM (
            SELECT partition_id, status, scheduler_session_id, session_id, create_ts
                  ,row_number() OVER (PARTITION BY scheduler_session_id,partition_id ORDER BY create_ts desc) id
            FROM "` + tgt_db + `"."` + meta_schema + `".` +local_log + `
            WHERE status in ('`+status_begin+`','`+status_end+`','`+status_failure+`')
               AND method = '`+method_worker+`'
            )
         WHERE id=1
      )
      SELECT s.partition_id, nvl(l.session_id,0) worker_session_id, nvl(l.status,'`+status_wait+`')
      FROM  "` + tgt_db + `"."` + process_tmp + `"."` + table_scheduler+ `"  s
         LEFT OUTER JOIN worker_log l
            ON l.scheduler_session_id = s.scheduler_session_id 
               AND l.partition_id=s.partition_id
               AND l.create_ts > s.create_ts
      WHERE s.scheduler_session_id=current_session()
      ORDER BY s.partition_id
   `;
    
   loop_counter=0
   while (true) {
      counter=0;
      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
      while (ResultSet.next()) {
         partition_id_tmp= ResultSet.getColumnValue(1);
         worker_session_id=ResultSet.getColumnValue(2);
         worker_status=    ResultSet.getColumnValue(3);
         if (worker_status==status_failure) {
            log("   WORKER "+partition_id_tmp+" FAILED" );
            drop_all_workers(process);
            throw new Error("WORKER FAILED") 
         } else if (worker_session_id == 0) {            
            counter+=1;
            log("   WAITING FOR WORKER "+partition_id_tmp);
         } else {
            if (worker_status == status_begin) {
               counter+=1;
               log("   WORKER FOR PARTITION "+partition_id_tmp+" STARTED");
            } else if (worker_status == status_end)  {
               log("   WORKER FOR PARTITION "+partition_id_tmp+" COMPLETED");
            } else {
               log("UNKNOWN WORKER STATUS "+worker_status)
               drop_all_workers(process);
               throw new Error("UNKNOWN WORKER STATUS "+worker_status+"; ABORT")
            }
         }
      }
      if (counter<=0)  {
         log("ALL WORKERS COMPLETED SUCCESSFULLY");
         drop_all_workers(process);
         break;
      } else {
         if (loop_counter<max_worker_wait_loop) {
            loop_counter+=1;
            log("   WAITING FOR "+counter+" WORKERS TO COMPLETE; LOOP CNT "+loop_counter);
            snowflake.execute({sqlText: "call /* "+loop_counter+" */ system$wait("+wait_to_poll+")"});
         } else {
            drop_all_workers(process);
            throw new Error("MAX WAIT REACHED; ABORT");
         }
      }                   
   } 
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-18
// Purpose     This function compacts all schemas to conly keep the number tables
//             pass in as a parameter. 
//             implemented methods.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

function compact(copies)
{
   log("COMPACT")

    var sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_drop_request  + `" AS 
         SELECT *
         FROM (
            (( -- all tables
               SELECT table_schema,table_name, curr_table_version,  curr_schema_name
               FROM "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `" 
               WHERE action in ('`+action_create+`','`+action_reference+`')
               GROUP BY table_schema,table_name, curr_table_version,  curr_schema_name 
            ) MINUS ( -- minus the already dropped tables
               SELECT table_schema,table_name, curr_table_version,  curr_schema_name
               FROM "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `" 
               WHERE action = '`+action_drop+`'
               GROUP BY table_schema,table_name, curr_table_version,  curr_schema_name 
            )) MINUS ( -- minus the tables for the last x copies
               SELECT table_schema,table_name, curr_table_version,  curr_schema_name
               FROM "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `" 
               WHERE run_id in 
                  (
                     SELECT  run_id
                     FROM "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `" 
                     WHERE action in ('`+action_create+`','`+action_reference+`')
                     GROUP BY run_id
                     ORDER BY run_id desc LIMIT `+copies+`
                  )
            )
         )
         ORDER BY table_schema,table_name
      `;

   snowflake.execute({sqlText: sqlquery});
   var sqlquery=`
      SELECT table_name, curr_table_version, curr_schema_name
      FROM "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_drop_request  + `"
      `;
   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
   while (ResultSet.next())  {
      counter = counter + 1;
      table_name = ResultSet.getColumnValue(1)+"_"+ResultSet.getColumnValue(2);
      schema_name = ResultSet.getColumnValue(3);
      var sqlquery=`
         DROP /* # ` + counter + ` */
         TABLE IF EXISTS "` + tgt_db + `"."` + schema_name + `"."` + table_name  + `"
         `;
      snowflake.execute({sqlText: sqlquery});
      log("   DROP TABLE: " + schema_name + "." + table_name);
   }

   sqlquery=`
   INSERT INTO "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `"
      SELECT date_part(epoch_milliseconds,convert_timezone('`+timezone+`',current_timestamp())) run_id
            ,hash(run_id) request_id ,convert_timezone('`+timezone+`',current_timestamp()) request_ts
            ,l.table_schema,l.table_name
            ,prev_schema_name,prev_table_version,l.curr_schema_name,l.curr_table_version
            ,source_fingerprint,source_commit_ts,source_bytes,source_row_count
            ,target_fingerprint,target_commit_ts,target_bytes,target_row_count
            ,'`+action_drop+`'
            ,convert_timezone('`+timezone+`',current_timestamp()) create_ts 
            ,null fingerprint_check
      FROM "` + tgt_db + `".` + meta_schema + `.` + local_object_log +` l
      INNER JOIN "` + tgt_db + `"."` + scheduler_tmp + `"."` + object_drop_request  + `" d
         ON d.table_schema =l.table_schema AND d.table_name=l.table_name 
            AND d.curr_schema_name = l.curr_schema_name AND d.curr_table_version = l.curr_table_version
      WHERE l.action='`+action_create+`'
      `;
   snowflake.execute({sqlText: sqlquery});


   sqlquery=`
      WITH empty_schema AS 
         (
            SELECT schema_name
            FROM (
               (
                  SELECT curr_schema_name schema_name
                  FROM "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `"
                  WHERE action in ('`+action_create+`','`+action_reference+`')
                  GROUP BY schema_name
               ) MINUS (
                  SELECT curr_schema_name schema_name
                  FROM (
                     (
                        SELECT table_name, curr_schema_name, curr_table_version
                        FROM "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `"
                        WHERE action in ('`+action_create+`','`+action_reference+`')
                     ) MINUS (
                        SELECT table_name, curr_schema_name, curr_table_version
                        FROM "` + tgt_db + `"."` + meta_schema + `"."` + local_object_log  + `"
                        WHERE action='`+action_drop+`'
                     )
                  )
                  GROUP BY schema_name
               )
            )
         )
      SELECT e.schema_name
      FROM empty_schema e 
      INNER JOIN "` + tgt_db + `".information_schema.schemata i 
         ON i.catalog_name='`+tgt_db+`' AND i.schema_name = e.schema_name
   `;
   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
   counter=0;
   while (ResultSet.next())  {
      counter = counter + 1;
      schema_name = ResultSet.getColumnValue(1);
      var sqlquery=`
         DROP /* # ` + counter + ` */
         SCHEMA IF EXISTS "` + tgt_db + `"."` + schema_name + `"
         `;
      snowflake.execute({sqlText: sqlquery});
      log("   DROP SCHEMA: " + schema_name );
   }
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-18
// Purpose     This function analyzes the scope of work that needs to be done and
//             then schedules the work. 
//             In case there is only work for one worker process, the scheduler
//             performs the work instead of waiting for a worker to spin up.
//             The function creates a temporary schema which is being used to
//             to communicate with the worker processes
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------
function scheduler () 
{
   snowflake.execute({sqlText: "CREATE OR REPLACE TRANSIENT SCHEMA \"" + tgt_db + "\".\"" + scheduler_tmp + "\";"});

   var sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `".` + scheduler_tmp + `.` + object_snapshot_meta_data + ` (
         partition_id number(19,0)
         ,database_name varchar
         ,object_schema varchar
         ,object_name varchar
         ,fingerprint number(19,0)
         ,last_commit_ts timestamp_tz(9)
         ,bytes number(12,0)
         ,row_count number(12,0)
      )
   `;
   snowflake.execute({sqlText: sqlquery});

   var sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `".` + scheduler_tmp + `.` + task_partitioned_set + ` (
         partition_id number(19,0)
         ,set_id number(19,0)
         ,check_fingerprint varchar
         ,database_name varchar
         ,object_schema varchar
         ,object_name varchar
         ,next_schema_name varchar
         ,next_table_version varchar
         ,perform_sync varchar
      )
   `;
   snowflake.execute({sqlText: sqlquery});

   var sqlquery=`
      CREATE TABLE IF NOT EXISTS"` + tgt_db + `".` + meta_schema + `.` + local_object_log + ` (
         run_id number(19,0)
         ,request_id number(19,0)
         ,request_ts timestamp_tz(9)
         ,table_schema varchar
         ,table_name varchar
         ,prev_schema_name varchar
         ,prev_table_version varchar
         ,curr_schema_name varchar
         ,curr_table_version varchar
         ,source_fingerprint number(19,0)
         ,source_commit_ts timestamp_tz(9)
         ,source_bytes number(12,0)
         ,source_row_count number(12,0)
         ,target_fingerprint number(19,0)
         ,target_commit_ts timestamp_tz(9)
         ,target_bytes number(12,0)
         ,target_row_count number(12,0)
         ,action varchar
         ,create_ts timestamp_tz(9) default convert_timezone('`+timezone+`',current_timestamp)
         ,fingerprint_check varchar
      )
   `;
   snowflake.execute({sqlText: sqlquery});

   create_sync_task_list();

   sqlquery=`
      SELECT count(1)
      FROM (
         SELECT distinct partition_id
         FROM   "` + tgt_db + `"."` + scheduler_tmp + `"."` + task_partitioned + `" 
      ) 
   `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   if (ResultSet.next()) {
      var partition_count=ResultSet.getColumnValue(1); 
      if (partition_count == 0) {
         log("NOTHING TO DO")
      } else if (partition_count == 1) {
         log("PROCESSING TASKS FOR PARTITION 1");
         process_sync_tasks(1);
         record_work();
         compact(max_copies)
      } else if (partition_count <= max_partition) {
         wait_for_worker_completion(method_sync);
         record_work();
         compact(max_copies)
      } else {
         throw new Error ("TOO MANY PARTITIONS; ABORT");
      }
   } else {
      throw new Error("TASK TABLE NOT FOUND")
   }
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-09-18
// Purpose     This function determines what needs to be done. The list of objects
//             to be syncd is either coming from the source database information schema
//             or it can be provides via a view called SMART_SYNC_DELTA_CHANGE. If the 
//             view does not exist it is assumed that all objects have to be syncd, i.e.
//             it is no necessary to collect fingerprint information. The total list
//             is then filtered by the last_altered data of the source objects. Only objects
//             that have been altered since the last run will be syncd. The resulting list
//             will be partitioned into groups of equal work.
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------
function create_refresh_task_list () {

   log("CREATE REFRESH TASK LIST")

   if (requested_refresh_id > 0){
      var requested_run_id=requested_refresh_id;
      log("REQUESTED RUN_ID: "+requested_refresh_id)

   } else {

      log("FIND RUN_ID FOR RELATIVE REFRESH SET: "+requested_refresh_id)

      var sqlquery=`
         SELECT -seq4() id,run_id, to_varchar(min(target_commit_ts),'YYYY-MM-DD HH24:MI:SS')
                , to_varchar(max(target_commit_ts),'YYYY-MM-DD HH24:MI:SS')
         FROM "`+src_db+`".`+meta_schema+`.`+local_object_log+` l1
         WHERE action in ('`+action_create+`','`+action_reference+`')
            AND target_commit_ts > NVL((
               SELECT max(target_commit_ts)
               FROM "`+src_db+`".`+meta_schema+`.`+local_object_log+`
               WHERE action = '`+action_drop+`'
            ),'2000-01-01')
            AND NOT EXISTS (
               SELECT 1 
               FROM "`+src_db+`".`+meta_schema+`.`+local_object_log+` l2
               INNER JOIN "`+src_db+`".`+meta_schema+`.`+local_object_log+` l3
                  ON l3.table_schema=l2.table_schema
                     AND l3.table_name=l2.table_name
                     AND l3.curr_schema_name=l2.curr_schema_name
                     AND l3.curr_table_version=l2.curr_table_version
                     AND l3.action = '`+action_drop+`'
               WHERE l2.run_id = l1.run_id            
            ) 
         GROUP BY run_id
         ORDER BY run_id desc
      `;

      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

      counter=0
      while (ResultSet.next()) {
         counter+=1;
         var id=ResultSet.getColumnValue(1); 
         var run_id=ResultSet.getColumnValue(2);
         var refresh_begin=ResultSet.getColumnValue(3);
         var refresh_end=ResultSet.getColumnValue(4);
         
         log("REFRESH SET: "+id+" RUN ID: "+run_id+" BEGIN: "+refresh_begin+" END: "+refresh_end);
         if (id == requested_refresh_id) {
            requested_run_id=run_id;
            break;
         }
      } 
   }
   log("USING RUN_ID: "+requested_run_id)

   if (requested_run_id==0) {
      throw new Error("REQUESTED REFRESH ID "+requested_refresh_id+" NOT FOUND; TRY MORE RECENT REFRESH SET")
   }

   sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `".` +refresh_tmp + `.` + object_sync_task + ` AS
         SELECT table_schema object_schema, table_name object_name,target_bytes bytes
               , curr_schema_name,table_name || '_' || curr_table_version curr_table_name            
         FROM "`+src_db+`".`+meta_schema+`.`+local_object_log+`
         WHERE run_id = `+requested_run_id+`
         ORDER BY table_schema,table_name
   `;
   snowflake.execute({sqlText: sqlquery});

   sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `"."` + refresh_tmp + `".` + task_partitioned + ` AS
          SELECT trunc(((SUM(((nvl(bytes,0)/1000000000)*1)::float) OVER (ORDER BY bytes,object_schema,object_name RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) +
                        (row_number() OVER (ORDER BY bytes,object_schema,object_name RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))) /
                       ((SELECT (sum(nvl(bytes,0)/1000000000)*1+1)::float FROM "` + tgt_db + `".` +refresh_tmp + `.` + object_sync_task + `) +
                        (SELECT sum(1) FROM "` + tgt_db + `".` + refresh_tmp + `.` + object_sync_task + `))
                     ) * least(
                           ceil((SELECT SUM(1) FROM "` + tgt_db + `".` + refresh_tmp + `.` + object_sync_task + ` 
                                 )/`+refresh_min_jobs_per_cluster+`)
                              ,`+refresh_cluster_count+`))+1 partition_id, t.*
         FROM "` + tgt_db + `".` + refresh_tmp + `.` + object_sync_task + ` t
         ORDER BY partition_id, object_schema, object_name
   `;
   snowflake.execute({sqlText: sqlquery});

   return requested_run_id;
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-18
// Purpose     This function analyzes the scope of work that needs to be done and
//             then schedules the work. 
//             In case there is only work for one worker process, the scheduler
//             performs the work instead of waiting for a worker to spin up.
//             The function creates a temporary schema which is being used to
//             to communicate with the worker processes
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------
function refresh (requested_refresh_id) 
{
   var requested_run_id = 0;
   snowflake.execute({sqlText: "CREATE SHARE IF NOT EXISTS \"" + tgt_share + "\";"});
   snowflake.execute({sqlText: "CREATE OR REPLACE TRANSIENT SCHEMA \"" + tgt_db + "\".\"" + refresh_tmp + "\";"});
   snowflake.execute({sqlText: "CREATE TRANSIENT SCHEMA IF NOT EXISTS \"" + tgt_db + "\".\"" + meta_schema + "\";"});

   sqlquery = `
      GRANT USAGE ON DATABASE "` + tgt_db + `" TO SHARE "` + tgt_share + `"`;
   snowflake.execute({sqlText: sqlquery});

   sqlquery = `
      GRANT USAGE ON SCHEMA "` + tgt_db + `"."` + meta_schema + `" TO SHARE "` + tgt_share + `"`;
   snowflake.execute({sqlText: sqlquery});

   if (src_db != tgt_db) {
      sqlquery = `
         GRANT REFERENCE_USAGE ON DATABASE "` + src_db + `" TO SHARE "` + tgt_share + `"`;
      snowflake.execute({sqlText: sqlquery});
      sqlquery = `
         CREATE OR REPLACE SECURE VIEW "` + tgt_db + `"."` + meta_schema + `"."` + remote_object_log + `" AS
               SELECT * FROM "` + src_db + `"."` + meta_schema + `"."` +local_object_log+ `"`;
      snowflake.execute({sqlText: sqlquery});

      sqlquery = `
         GRANT SELECT ON TABLE "` + tgt_db + `"."` + meta_schema + `"."` +remote_object_log+ `" TO SHARE "` + tgt_share + `"`;
      snowflake.execute({sqlText: sqlquery});

      sqlquery = `
         CREATE OR REPLACE SECURE VIEW "` + tgt_db + `"."` + meta_schema + `"."` +remote_log+ `" AS
               SELECT * FROM "` + src_db + `"."` + meta_schema + `"."` +local_log+ `"`;
      snowflake.execute({sqlText: sqlquery});

      sqlquery = `
         GRANT SELECT ON TABLE "` + tgt_db + `"."` + meta_schema + `"."` +remote_log+ `" TO SHARE "` + tgt_share + `"`;
      snowflake.execute({sqlText: sqlquery});
   } else {
      sqlquery = `
         GRANT SELECT ON TABLE "` + tgt_db + `"."` + meta_schema + `"."` +local_object_log+ `" TO SHARE "` + tgt_share + `"`;
      snowflake.execute({sqlText: sqlquery});
      sqlquery = `
         GRANT SELECT ON TABLE "` + tgt_db + `"."` + meta_schema + `"."` +local_log+ `" TO SHARE "` + tgt_share + `"`;
      snowflake.execute({sqlText: sqlquery});
   }

   requested_run_id=create_refresh_task_list();

   sqlquery=`
      SELECT count(1)
      FROM (
         SELECT distinct partition_id
         FROM   "` + tgt_db + `"."` + refresh_tmp + `"."` + task_partitioned + `" 
      ) 
   `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   if (ResultSet.next()) {
      var partition_count=ResultSet.getColumnValue(1); 
      if (partition_count == 0) {
         log("NOTHING TO DO")
      } else if (partition_count == 1) {
         log("PROCESSING TASKS FOR PARTITION 1");
         process_refresh_tasks(1);
      } else if (partition_count <= max_partition) {
         wait_for_worker_completion(method_refresh);
      } else {
         throw new Error ("TOO MANY PARTITIONS; ABORT");
      }
   } else {
      throw new Error("TASK TABLE NOT FOUND")
   }

   log("SWAP SCHEMA");

   sqlquery=`
      SELECT table_schema
      FROM "`+src_db+`".`+meta_schema+`.`+local_object_log+`
      WHERE run_id = `+requested_run_id+`
      GROUP BY table_schema
      ORDER BY table_schema
   `;
   var tableNameResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   counter=0;
   
   while (tableNameResultSet.next()) {
      counter = counter + 1;
      table_schema=tableNameResultSet.getColumnValue(1);

      log("   SWAP SCHEMA: "+table_schema+" WITH SCHEMA: "+internal+"_"+table_schema);
      sqlquery = `
         ALTER SCHEMA "` + tgt_db + `"."` + table_schema + `" 
         SWAP WITH "` + tgt_db + `"."` + internal+`_`+table_schema + `"`;
      snowflake.execute({sqlText: sqlquery});
       
      sqlquery = `
         DROP SCHEMA "` + tgt_db + `"."` + internal+`_`+table_schema + `"`;
      snowflake.execute({sqlText: sqlquery});
   }
}

// -----------------------------------------------------------------------------
// Author      Robert Fehrmann
// Created     2020-08-01
// Purpose     This function is the main function. It calls the processing for the 
//             implemented methods.
//             It implements a switch based on the method parameter and call the 
//             appropriate function
// Copyright (c) 2020 Snowflake Inc. All rights reserved
// -----------------------------------------------------------------------------

try {
   snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + tgt_db + "\"." + meta_schema + ";"});
   // create one row per configured number of clusters with 1 minute in between
   sqlquery=`
      SELECT current_warehouse(),convert_timezone('`+timezone+`',current_timestamp) 
   `;

   var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

   if (ResultSet.next()) {
      current_warehouse=ResultSet.getColumnValue(1);
      current_timestamp=ResultSet.getColumnValue(2) 
   } else {
      throw new Error ("WAREHOUSE NOT FOUND");
   }
                                                                     
   if (method==method_sync) {
      log("procName: " + procName + " " + status_begin);
      flush_log(status_begin);

      scheduler();

      log("procName: " + procName + " " + status_end);
      flush_log(status);
      return return_array;

   } else if (method==method_worker_sync){

      snowflake.execute({sqlText: "ALTER TASK \""+tgt_db+"\"."+scheduler_tmp+"."+task_name_worker_sync+"_"+worker_id+" suspend"});

      sqlquery=`
         WITH worker_log 
            AS (
               SELECT partition_id, status, scheduler_session_id, session_id, create_ts
               FROM (
                  SELECT partition_id, status, scheduler_session_id, session_id, create_ts
                        ,row_number() OVER (PARTITION BY scheduler_session_id,partition_id ORDER BY create_ts desc) id
                  FROM "` + tgt_db + `"."` + meta_schema + `".` + local_log + `
                  WHERE status in ('`+status_begin+`','`+status_end+`','`+status_failure+`')
                     AND method = '`+method_worker_sync+`'
                  )
               WHERE id=1
            )           
         SELECT s.scheduler_session_id,s.partition_id, nvl(l.session_id,0) worker_session_id
         FROM "` + tgt_db + `".` + scheduler_tmp + `.` + table_scheduler+` s
            LEFT OUTER JOIN worker_log l 
               ON l.scheduler_session_id = s.scheduler_session_id AND l.partition_id=s.partition_id AND l.create_ts > s.create_ts
         WHERE s.partition_id = `+worker_id+`
      `;

      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

      if(ResultSet.next()){
         scheduler_session_id = ResultSet.getColumnValue(1);
         partition_id = ResultSet.getColumnValue(2);
         worker_session_id=ResultSet.getColumnValue(3)

         log("procName: " + procName + " " + status_begin);
         flush_log(status_begin);

         if (worker_session_id == 0) {
            log("PROCESSING TASKS FOR SCHEDULER ID "+scheduler_session_id+" PARTITION "+partition_id);
            process_sync_tasks(partition_id);
         } else {
            log("PROCESSING FOR SCHEDULER ID "+scheduler_session_id+" PARTITION "+partition_id+" ALREADY DONE BY SESSION "+worker_session_id);
         }

         log("procName: " + procName + " " + status_end);
         flush_log(status_end);
         return return_array;

      } else {
         throw new Error ("PARTITION NOT FOUND")
      }
   } else if (method==method_refresh) {
      log("procName: " + procName + " " + status_begin);
      flush_log(status_begin);

      refresh(requested_refresh_id);

      log("procName: " + procName + " " + status_end);
      flush_log(status_end);
      return return_array;
   } else if (method==method_worker_refresh){

      snowflake.execute({sqlText: "ALTER TASK \""+tgt_db+"\"."+refresh_tmp+"."+task_name_worker_refresh+"_"+worker_id+" suspend"});

      sqlquery=`
         WITH worker_log 
            AS (
               SELECT partition_id, status, scheduler_session_id, session_id, create_ts
               FROM (
                  SELECT partition_id, status, scheduler_session_id, session_id, create_ts
                        ,row_number() OVER (PARTITION BY scheduler_session_id,partition_id ORDER BY create_ts desc) id
                  FROM "` + tgt_db + `"."` + meta_schema + `".` + local_log + `
                  WHERE status in ('`+status_begin+`','`+status_end+`','`+status_failure+`')
                     AND method = '`+method_worker_refresh+`'
                  )
               WHERE id=1
            )           
         SELECT s.scheduler_session_id,s.partition_id, nvl(l.session_id,0) worker_session_id
         FROM "` + tgt_db + `".` + refresh_tmp + `.` + table_scheduler+` s
            LEFT OUTER JOIN worker_log l 
               ON l.scheduler_session_id = s.scheduler_session_id AND l.partition_id=s.partition_id AND l.create_ts > s.create_ts
         WHERE s.partition_id = `+worker_id+`
      `;

      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

      if(ResultSet.next()){
         scheduler_session_id = ResultSet.getColumnValue(1);
         partition_id = ResultSet.getColumnValue(2);
         worker_session_id=ResultSet.getColumnValue(3)

         log("procName: " + procName + " " + status_begin);
         flush_log(status_begin);

         if (worker_session_id == 0) {
            log("PROCESSING TASKS FOR SCHEDULER ID "+scheduler_session_id+" PARTITION "+partition_id);
            process_refresh_tasks(partition_id);
         } else {
            log("PROCESSING FOR SCHEDULER ID "+scheduler_session_id+" PARTITION "+partition_id+" ALREADY DONE BY SESSION "+worker_session_id);
         }

         log("procName: " + procName + " " + status_end);
         flush_log(status_end);
         return return_array;
      } else {
         throw new Error ("PARTITION NOT FOUND")
      }
   } else if (method==method_compact) {
      log("procName: " + procName + " " + status_begin);
      flush_log(status_begin);

      compact(copies_to_keep);

      log("procName: " + procName + " " + status_end);
      flush_log(status_end);
      return return_array;

   } else {
      throw new Error("REQUESTED METHOD NOT FOUND; "+method);
   }
}
catch (err) {
   log("ERROR found - MAIN try command");
   log("err.code: " + err.code);
   log("err.state: " + err.state);
   log("err.message: " + err.message);
   log("err.stacktracetxt: " + err.stacktracetxt);
   log("procName: " + procName );
   flush_log(status_failure);
   return return_array;
}
$$;