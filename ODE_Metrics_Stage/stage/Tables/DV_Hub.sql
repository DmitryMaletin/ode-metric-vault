CREATE TABLE [stage].[DV_Hub] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [hub_key]                INT                NOT NULL,
    [hub_name]               VARCHAR (128)      NOT NULL,
    [hub_abbreviation]       VARCHAR (4)        NULL,
    [hub_schema]             VARCHAR (128)      NOT NULL,
    [hub_database]           VARCHAR (128)      NOT NULL,
    [is_retired]             BIT                NOT NULL,
    [is_compressed]          BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [release_number]         INT                NULL,
    [version_number]         INT                NOT NULL,
    [updated_by]             VARCHAR (128)       NULL,
    [updated_datetime]       DATETIMEOFFSET (7) NULL
);



