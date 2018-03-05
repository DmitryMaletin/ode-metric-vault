CREATE TABLE [stage].[DV_Run_Manifest] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [run_manifest_key]       INT                IDENTITY (1, 1) NOT NULL,
    [run_key]                INT                NOT NULL,
    [source_unique_name]     VARCHAR (128)      NOT NULL,
    [source_table_load_type] VARCHAR (50)       NOT NULL,
    [source_table_key]       INT                NOT NULL,
    [priority]               VARCHAR (10)       NOT NULL,
    [queue]                  VARCHAR (10)       NOT NULL,
    [start_datetime]         DATETIMEOFFSET (7) NULL,
    [completed_datetime]     DATETIMEOFFSET (7) NULL,
    [run_status]             VARCHAR (128)      NOT NULL,
    [row_count]              INT                NOT NULL,
    [session_id]             INT                NULL
);



