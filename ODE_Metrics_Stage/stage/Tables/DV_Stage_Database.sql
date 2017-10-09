CREATE TABLE [Stage].[DV_Stage_Database] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [stage_database_key]     INT                NOT NULL,
    [stage_database_name]    VARCHAR (50)       NOT NULL,
    [stage_connection_name]  VARCHAR (50)       NULL,
    [is_retired]             BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [version_number]         INT                NULL,
    [updated_by]             VARCHAR (128)      NULL,
    [update_date_time]       DATETIMEOFFSET (7) NULL,
    [release_number]         INT                NULL
);

