CREATE TABLE [stage].[DV_Release] (
    [metrics_stage_run_time]    DATETIMEOFFSET (7) NOT NULL,
    [release_number]            INT                NOT NULL,
    [release_description]       VARCHAR (256)      NULL,
    [reference_number]          VARCHAR (50)       NULL,
    [reference_source]          VARCHAR (50)       NULL,
    [build_number]              INT                NOT NULL,
    [build_date]                DATETIMEOFFSET (7) NULL,
    [build_server]              VARCHAR (256)      NULL,
    [release_built_by]          VARCHAR (128)       NULL,
    [release_start_datetime]    DATETIMEOFFSET (7) NULL,
    [release_complete_datetime] DATETIMEOFFSET (7) NULL,
    [release_count]             INT                NOT NULL,
    [version_number]            INT                NOT NULL,
    [updated_by]                VARCHAR (128)       NOT NULL,
    [updated_datetime]          DATETIMEOFFSET (7) NOT NULL
);

