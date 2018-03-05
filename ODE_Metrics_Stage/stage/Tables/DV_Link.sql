CREATE TABLE [stage].[DV_Link] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [link_key]               INT                NOT NULL,
    [link_name]              VARCHAR (128)      NOT NULL,
    [link_abbreviation]      VARCHAR (4)        NULL,
    [link_schema]            VARCHAR (128)      NOT NULL,
    [link_database]          VARCHAR (128)      NOT NULL,
    [is_compressed]          BIT                NOT NULL,
    [is_retired]             BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [release_number]         INT                NULL,
    [version_number]         INT                NOT NULL,
    [updated_by]             VARCHAR (128)       NULL,
    [updated_datetime]       DATETIMEOFFSET (7) NULL
);



