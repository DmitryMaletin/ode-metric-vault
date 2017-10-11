CREATE TABLE [Stage].[DV_Source_Table] (
    [metrics_stage_run_time]       DATETIMEOFFSET (7) NOT NULL,
    [source_table_key]             INT                NULL,
    [source_system_key]            INT                NULL,
    [source_system_is_retired]     BIT                NULL,
    [source_system_name]           VARCHAR (50)       NULL,
    [source_system_release_key]    INT                NULL,
    [source_system_release_number] INT                NULL,
    [source_table_schema]          VARCHAR (128)      NULL,
    [source_table_name]            VARCHAR (128)      NULL,
    [source_unique_name]           VARCHAR (128)      NULL,
    [source_table_load_type]       VARCHAR (50)       NULL,
    [is_retired]                   BIT                NULL,
    [release_key]                  INT                NULL,
    [release_number]               INT                NULL,
    [updated_by]                   VARCHAR (128)      NULL,
    [update_date_time]             DATETIMEOFFSET (7) NULL
);





