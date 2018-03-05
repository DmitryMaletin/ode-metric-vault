CREATE TABLE [stage].[DV_Schedule] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [schedule_key]           INT                NOT NULL,
    [schedule_name]          VARCHAR (128)      NOT NULL,
    [schedule_description]   VARCHAR (256)      NULL,
    [schedule_frequency]     VARCHAR (128)      NOT NULL,
    [is_cancelled]           BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [release_number]         INT                NULL,
    [version_number]         INT                NOT NULL,
    [updated_by]             VARCHAR (128)       NOT NULL,
    [updated_datetime]       DATETIMEOFFSET (7) NOT NULL
);

