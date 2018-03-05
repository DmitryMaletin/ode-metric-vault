CREATE TABLE [Stage].[DV_Source_Version] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [source_version_key]     INT                NOT NULL,
    [source_table_key]       INT                NOT NULL,
    [source_version]         INT                NULL,
    [source_type]            VARCHAR (50)       NOT NULL,
    [source_procedure_name]  VARCHAR (128)      NULL,
    [source_filter]          VARCHAR (4000)     NULL,
    [pass_load_type_to_proc] BIT                NOT NULL,
    [is_current]             BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [version_number]         INT                NULL,
    [updated_by]             VARCHAR (128)      NULL,
    [update_date_time]       DATETIMEOFFSET (7) NULL,
    [release_number]         INT                NULL
);

