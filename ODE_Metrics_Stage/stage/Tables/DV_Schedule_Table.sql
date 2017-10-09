CREATE TABLE [stage].[DV_Schedule_Table] (
    [metrics_stage_run_time]    DATETIMEOFFSET (7) NOT NULL,
    [schedule_source_table_key] INT                NOT NULL,
    [schedule_key]              INT                NOT NULL,
    [source_table_key]          INT                NOT NULL,
    [source_table_load_type]    VARCHAR (50)       NOT NULL,
    [priority]                  VARCHAR (50)       NOT NULL,
    [queue]                     VARCHAR (50)       NOT NULL,
    [is_cancelled]              BIT                NOT NULL,
    [release_key]               INT                NOT NULL,
    [release_number]            INT                NULL,
    [version_number]            INT                NOT NULL,
    [updated_by]                VARCHAR (128)       NOT NULL,
    [updated_datetime]          DATETIMEOFFSET (7) NOT NULL
);

