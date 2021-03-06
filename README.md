# smartSync - Consuming Shared Data in a Virtual Private Snowflake Account 

## Overview

Snowflake has an amazing feature called [Secure Data Sharing](https://www.snowflake.com/use-cases/modern-data-sharing/). With Snowflake Secure Data Sharing, two accounts in the same cloud region and the same CSP (Cloud Service Provider) can share data in an instant and secure way. Data Sharing is possible because of Snowflake's unique architecture, that separates storage and compute. Because of this architecture, a data provider can configure access to it's data by creating a share. Think of a share as a collection of all the necessary metadata, for instance, names of shared objects, location of the data files, methods to decrypt the data, and so on. However, data can only be shared directly between two Snowflake accounts that exist in the same Region and the same CSP. Data still can be shared between accounts in different regions with the same CSP, or with different CSPs (regardless of geographical region), but this requires data to be replicated. Please review the [documentation](https://docs.snowflake.com/en/user-guide/secure-data-sharing-across-regions-plaforms.html) for more details on database replication. 

By design, a [Virtual Private Snowflake](https://docs.snowflake.com/en/user-guide/intro-editions.html#virtual-private-snowflake-vps) a VPS is considered its own region. For that reason, sharing data into a VPS Account requires the data from the provider side to be replicated into the VPS account. Then we can share the local copy of the dataset inside VPS by creating a local share.

### Sync Step

As mentioned in the documentation above, a database created from a share can not be used as a source for replication. Only a database that is "local" to the current account can be replicated. Therefore, if we want to consume shared data in a VPS account, we first have to create a local copy of the shared dataset and then we can replicate that local copy into the VPS account. On the surface, creating a local copy seems to be very straight forward. 

* create a local table via a CTAS (CREATE TABLE AS) statement into a new schema in a new database
* replicate that local database into the VPS account
* share the now replicated database to as many account inside the VPS as you desire
* run the whole process on a regular schedule

Though this will work, there are several challenges

* how do we handle the process when there are hundreds or thousands of objects (tables/views)?
* how do we handle bigger tables with 10th of GB of data and hundreds of millions or rows?
* how do we handle consistency since it takes time to copy the share object by object?
* how do we limit replication to the bare minimum since cross region / cross cloud data replication is costly?

SmartSync is a Snowflake Procedures that handles all of the above challenges.

* Automate the copy process from source schema (share) to target schema (local)
* Collect metadata information like (list of objects copied and their state (data as well as structure)) 
* Analyze metadata information from previous step to limit data changes to a minimum and create execution plan
* Execute execution plan from previous step 
* Collect metadata information again and compare metadata sets for differences (potential consistency problems)
* Record metadata information (tables, performed actions, row counts, fingerprints) for auditibility

SmartSync stores the data for the local copy in a database with the name of the source schema's appended by a date. Source objects are copied to a target object with a sequential number (snapshot number) (6 digit). 

The sync step is invoked by calling [sp_sync](#sp_sync) with the sync method. The sync method creates a new set of target tables based on all objects (tables/views) that have changed in the source (shared) database. To ensures that the consumption layer (see [Sharing Step](#Sharing-Step)) is not interrupted during the sync process (which can run for an extended period of time), SmartSync always creates new target tables.  

### Replication Step

With the ability to create a local copy of a shared dataset, we can replicate the local copy into the VPS deployment via standard Snowflake replication. Setup of replication of the local database is currently not part of smart sync. Details on how to setup replication can be found [here](https://docs.snowflake.com/en/user-guide/database-replication-config.html#). 

### Sharing Step 

The local copy (or replicated copy) of the shared dataset can now be shared to consumer account inside the VPS. For that we have to create a set of secure views pointing to the new local copies of the shared dataset. Smart sync supports building the secure view abstraction layer through the "REFRESH" method.

## Implementation Interface

SmartSync is implemented in a single stored procedure that basically acts as a library. SmartSync stores all internal data in a metadata repository, i.e. schema smart_sync_metadata) in the target database. By default all *dates* in SmartSync are UTC. If you like to change the default behavior, please see [customizations](#Customizations).

There are to metadata tables:
* LOG: The log table acts as an execution log. There are 2 log records per call to sp_sync, i.e. a *begin* record and a *end*/*failure* record. A detailed execution log is provided in the *message* attribute.
*OBJECT_LOG: The ObjectLog provides a detail record for each sync process for each source object (table/secure view). Data in object log is append only, i.e. there are different records for creation and the deletion of objects. 
Collected metadata includes
 * Run ID (sequentially increasing number) 
 * Request metadata (ID and request timestamp)
 * Action (CREATE/REFERENCE/DROP) (CREATE/DROP correspond to their SQL statement equivalent. REFERENCE indicates that a particular table has not been modified between the current and the previous run. Therefor, no new object has been created but the old object is referenced)
 * Source object key (schema name, table_name)
 * Target object location (current and previous schema name and table version)
 * Source object metadata (commit timestamp, number of rows, table size in bytes (only for tables)) 
 * Target object metadata (commit timestamp, number of rows, table size in bytes )
 * Creation Timestamp (this is the completion timestamp of the call since object log data is collected as the last step of the process)

All calls to the sp_sync stored procedure have 4 parameters.
* Method
    * SYNC: The SYNC method performs an analysis regarding what objects have changed. To run the actual sync process in parallel, it partitions all tables to be syncd into N groups and then creates a [TASK](https://docs.snowflake.com/en/user-guide/tasks-intro.html) for each partition. Then it waits (synchroniously) for completion of all tasks. After successful completion of all tasks or a failure of at least one task, all tasks will be removed. The degree of parallelizm is set via the Method parameter (see below). Increase the degree of parallelizm to decrease the runtime but keep in mind that the minimum runtime is determined by the processing time for the single largest table. Increasing the parallelizm beyond that point only creates a long tail and doesn't provide additional benefits.
    To ensure that the target database does not continiously grow, the COMPACT method is called to remove target tables for older runs. The default number of kept runs can be changed in [customizations](#Customizations).
    * COMPACT: The COMPACT method removes all target tables (snapshots) for older synchronization runs. The number of snapshots to keep is provided via the Method Parameter.
    * REFRESH: The REFRESH method creates a secure view abstractions layer pointing to a list of target objects created of referenced by a specific RunID. The RunID (positive number) is provided via the Method Parameter. If the Method Parameter is 0 or negaative, it is interpreted as a relative RunID, i.e. 0=most recent run, -1=previous run, ...). 
    * WORKER_SYNC (INTERNAL ONLY):
    The WORKER_SYNC method is designed as an internal method. It expects several temporary tables to be available and therefor it is not recommended to be called directly.  
* Method Parameter
    * Method specific numeric value, i.e. degree of parallelizm, RunIDs to keep, RunID to expose via secure view abstraction layer)
* Source Database
    * For the SYNC Method, the source database is the database created from the shared provided by the data provider. * * For the REFRESH Method, the source database is the replicated target database (for a remote scenario), or the target database (in case you want to create a sharable abstraction layer in the local environment)
* Target Database 
    * For the SYNC Method, the target database is the local database where the sync process will create the target tables. 
    * For the REFRESH Method, the target database is where the refres process creates the secure view abstraction layer. Source and Target database can be the same. In that case the secure view abstraction layer is created along side with the target tables. 

### SP_SYNC
    
    create or replace procedure SP_SYNC(
        I_METHOD VARCHAR
        ,I_METHOD_PARAM FLOAT
        ,I_SRC_DB VARCHAR
        ,I_TGT_DB VARCHAR
    )

### Customizations

There are several customizations you can make by modifying parameters in the source code. Keep in mind that those changes will be overriden in case your deploy again from the github repo.

* timezone='UTC'; All times are adjusted to the same timezone, i.e. UTC. You can change the timezone, i.e. to work in US/Eastern timezone.
* max_copies=14; This is the maximum number of snapshots kept. Snapshot older than this number will automatically be deleted upon a sync run. You can change this number to keep more snapshots. Keep in mind that every snapshot consumes additional space. 
* smart_sync_db="SMART_SYNC_DB"; This is the default database name for the code repository. 
* smart_sync_meta_schema="METADATA"; This is the default schema for the code reposity. 


## Setup

1. Clone the SmartSync repo (use the command below or any other way to clone the repo)
    ```
    git clone https://github.com/RobertFehrmann/smartSync.git
    ```   
1. Create database and role to host stored procedures. Both steps require the AccountAdmin role (unless your current role has the necessary permissions.
    ``` 
    use role AccountAdmin;
    drop role if exists smart_sync_rl;
    drop database if exists smart_sync_db;
    create role smart_sync_rl;
    grant create share on account to role smart_sync_rl;
    grant execute task on account to role smart_sync_rl;
    create database smart_sync_db;
    grant usage on database smart_sync_db to role smart_sync_rl;
    ``` 
1. Grant smart_sync_role to the appropriate user (login). Replace `<user>` with the user you want to use for smart_copy. Generally speaking, this should be the user you are connected with right now. Note that you also could use the AccountAdmin role for all subsequent steps. That could be appropriate on a test or eval system but not for a production setup.
    ```
    use role AccountAdmin;
    grant role smart_sync_rl to user <user>;
    create schema smart_sync_db.metadata;
    grant usage on schema smart_sync_db.metadata to role smart_sync_rl;
    use database smart_sync_db;
    use schema smart_sync_db.metadata;
    ```
1. Create procedure sp_sync from the metadata directory inside the cloned repo by loading the file into a worksheet and then clicking `Run`. Note: If you are getting an error message (SQL compilation error: parse ...), move the cursor to the end of the file, click into the window, and then click `Run` again). Then grant usage permissions on the created stored procs.
    ```
    use role AccountAdmin;
    grant usage on procedure smart_sync_db.metadata.sp_sync(varchar,float,varchar,varchar) to role smart_sync_rl;
    ```

## Operations (source side)

The following steps need to be executed for every database. Note: [Setup Steps](#Setup) listed above need to be executed before you can start this section.

1. Though it's not required, it is recommended to run every sync setup(database) with it's own dedicted warehouse. Set MAX_CLUSTER_COUNT to the appropriate value based on the size of the biggest object, number of objects and desired runtime SLA. For instance, you can expect to run 1 degrees of parallelizm per cluster. To avoid a long tail problem, i.e. the minimum run time is determined by the largest object (table/view), do not increase the degree of parallelizm when the worker processes with only one object to process.
    Note: If you grant "modify" to the custom role, SmartSync allocates all required clusters before task processing starts. This has a positive impact on overall runtime since SmartSync doesn't have to wait for the scale-out events. 
    Getting sizing information from the data provider helps you to decide what warehouse size to use. Take the biggest (by size) shared table and start small. Tables below 10 GB work well with XSMALL, less than 100 GB => SMALL, less than 1 TB =>MEDIUM.
    ```
    use role accountadmin;
    drop warehouse if exists smart_sync_<warehouse>;
    create warehouse smart_sync_<warehouse> with 
       WAREHOUSE_SIZE = XSMALL 
       MAX_CLUSTER_COUNT = <X>
       SCALING_POLICY = STANDARD
       AUTO_SUSPEND = 15 
       AUTO_RESUME = TRUE
       MAX_CONCURRENCY_LEVEL=2;
    grant usage,operate,monitor,modify on warehouse smart_sync_<warehouse> to role smart_sync_rl;
    ```
1. Create the target (local) database, and grant the necessary permission to role smart_sync_rl
    ```
    use role AccountAdmin;
    drop database if exists <local db>;
    create database <local db>;
    grant all on database <local db> to role smart_sync_rl with grant option;
    grant ownership on schema <local db>.public to role smart_sync_rl;
    ```
1. Create the target (shared) database, and grant the necessary permission to role smart_sync_rl
    ```
    use role AccountAdmin;
    drop database if exists <target shared db>;
    create database <target shared db>;
    grant all on database <target shared db> to role smart_sync_rl with grant option;
    grant ownership on schema <target shared db>.public to role smart_sync_rl;
    ```
1. Create the source database from the share and grant the necessary permission to role smart_sync_rl
    ```
    use role AccountAdmin;
    drop database if exists <source db>;
    create database <source db> from share <provider account>.<source db>;
    grant imported privileges on database <source db> to role smart_sync_rl;
    ```
1. (Optional) Smart Sync supports a delta sync concept by providing a view that lists all tables to be syncd. If a delta sync table is provided, SmartSync syncs exactly the objects listed in the view. SmartSync will not create a fingerprint for the source tables and therefor processing can be faster in case source tables are very big or the number of changed tables is considerably smaller then the total number of tables.  
    Use the initial sync template from folder provider/crux to limit the secure views to re synced to the desired list. Set the date to the previous day. This ensures that SmartSync finds the most recent copy of all objects to be synced. Run the sync command (see below) to initiate the initial sync.
    ```
    use role smart_sync_rl;
    create schema <local db>.SMART_SYNC_METADATA;
    create view <local db>.SMART_SYNC_METADATA.SMART_SYNC_DELTA_CHANGE
        as select * ... (take delta_sync_initial template from folder provider/Crux
    ```
    After a successful initial sync use the delta_sync template from folder provider/crux and override the delta sync view from the prvious step. Note that the date filter is now going from the current date forward. This ensures that SmartSync finds the most recent copy of all objects to be synced. Run the sync command (see below) to initiate the first delta sync. Be sure to set your context, i.e. role and warehouse (from above).
    ```
    use role smart_sync_rl;
    create schema if not exists <local db>.SMART_SYNC_METADATA;
    create view <local db>.SMART_SYNC_METADATA.SMART_SYNC_DELTA_CHANGE
        as select * ... (take delta_sync template from folder provider/Crux
    ```
1. Run the sync command. The degree of parallelism depends again on the shared dataset. By default the number of parallel tasks is set to 4. This works well for databases with less than 500 tables. 2000 Tables work well with 10 parallel tasks.
    ```
    use role smart_sync_rl;
    use <warehouse>
    call smart_sync_db.metadata.sp_sync('SYNC',<degree of parallelizm>,<shared db>,<local db>);
    ```
1. Run the refresh command
    ```
    use role smart_sync_rl;
    use <warehouse>;
    call smart_sync_db.metadata.sp_sync('REFRESH',0,<local db>,<target shared db>);
    ```
1. Create the necessary tasks to run the steps on a regular schedule. The defaults below schedule the tasks at 4:00 AM EST on a daily basis. Modify the schedule as needed. For consistency purposes, create the tasks in the local database.     
    ```
    use role smart_sync_rl;
    create or replace task <local db>.smart_sync_metadata.<sync task>
      WAREHOUSE = <warehouse>
      SCHEDULE = 'USING CRON 0 4 * * * US/Eastern'
      USER_TASK_TIMEOUT_MS = 10800000
      AS 
        call smart_sync_db.metadata.sp_sync('SYNC',<degree of parallelism>,'<shared db'>,'<local db>');

    create or replace task <local db>.smart_sync_metadata.<refresh task>
      WAREHOUSE = <warehouse>
      USER_TASK_TIMEOUT_MS = 10800000
      AFTER <local db>.smart_sync_metadata.<sync task>
      AS 
        call smart_sync_db.metadata.sp_sync('REFRESH',0,'<local db>','<target shared db>');

    
    alter task  <local db>.smart_sync_metadata.<refresh task> resume;
    alter task  <local db>.smart_sync_metadata.<sync task> resume; 
    ```
1. The last step on the source side is to enable replication for the local database so the target side.
    ```
    alter database <local db> enable replication to accounts <remote region>.<remote account>;
    ```

## Operations (target side)

The following steps need to be executed for every database to be sync'd. Note: [Setup Steps](#Setup) listed above need to be executed before you can start this section.

1. Though it's not required, it is recommended to run every sync setup(database) with it's own dedicted warehouse. Set MAX_CLUSTER_COUNT to 4.
    ```
    use role accountadmin;
    drop warehouse if exists smart_sync_<warehouse>;
    create warehouse smart_sync_<warehouse> with 
       WAREHOUSE_SIZE = XSMALL 
       MAX_CLUSTER_COUNT = <X>
       SCALING_POLICY = STANDARD
       AUTO_SUSPEND = 15 
       AUTO_RESUME = TRUE
       MAX_CONCURRENCY_LEVEL=2;
    grant usage,operate,monitor,modify on warehouse smart_sync_<warehouse> to role smart_sync_rl;
    ```
1. Create the target (local) database, and grant the necessary permission to role smart_sync_rl. If you need to drop the local database, you may have to delete dependent objects. 
    ```
    use role smart_sync_rl;
    drop share if exists <shared_database>_share;
    drop database if exists <local db>;
    use role AccountAdmin;
    create database <local db> as replica of <source region>.<source account>.<source local database>;
    grant ownership on database <local db> to role smart_sync_rl;
    ```
1. Create the target (shared) database, and grant the necessary permission to role smart_sync_rl
    ```
    use role AccountAdmin;
    drop database if exists <shared db>;
    create database <shared db>;
    grant all on database <shared db> to role smart_sync_rl with grant option;
    grant ownership on schema <target shared db>.public to role smart_sync_rl;
    use role smart_sync_rl;
    create schema <shared db>.smart_sync_metadata;
    ```
1. Create the necessary tasks to run the steps on a regular schedule. The defaults below schedule the tasks to run every hour on the hour. Modify the schedule as needed. For consistency purposes, create the tasks in the shared database since hte local database is read only.     
    ```
    use role smart_sync_rl;
    create or replace task <shared db>.smart_sync_metadata.<database refresh task>
      WAREHOUSE = <warehouse>
      SCHEDULE = 'USING CRON 0 * * * * US/Eastern'
      USER_TASK_TIMEOUT_MS = 10800000
      AS 
        alter database <local db> refresh;

    create or replace task <shared db>.smart_sync_metadata.<refresh task>
      WAREHOUSE = <warehouse>
      USER_TASK_TIMEOUT_MS = 10800000
      AFTER <shared db>.smart_sync_metadata.<database refresh task>
      AS 
        call smart_sync_db.metadata.sp_sync('REFRESH',0,'<local db>','<shared db>');

    
    alter task  <shared db>.smart_sync_metadata.<refresh task> resume;
    alter task  <shared db>.smart_sync_metadata.<database refresh task> resume; 
    ```


