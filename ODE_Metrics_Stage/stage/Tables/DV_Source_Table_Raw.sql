CREATE TABLE [Stage].[DV_Source_Table_Raw] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [source_table_key]       INT                NOT NULL,
    [system_key]             INT                NULL,
    [source_unique_name]     VARCHAR (128)      NOT NULL,
    [load_type]              VARCHAR (50)       NOT NULL,
    [source_table_schma]     VARCHAR (128)      NULL,
    [source_table_nme]       VARCHAR (128)      NULL,
    [stage_schema_key]       INT                NULL,
    [stage_table_name]       VARCHAR (128)      NULL,
    [is_columnstore]         BIT                NOT NULL,
    [is_compressed]          BIT                NOT NULL,
    [is_retired]             BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [release_number]         INT                NULL,
    [version_number]         INT                NULL,
    [updated_by]             VARCHAR (128)      NULL,
    [update_date_time]       DATETIMEOFFSET (7) NULL
);





