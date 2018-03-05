CREATE TABLE [Stage].[DV_Object_Match] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [match_key]              INT                NOT NULL,
    [source_version_key]     INT                NOT NULL,
    [temporal_pit_left]      DATETIMEOFFSET (7) NULL,
    [temporal_pit_right]     DATETIMEOFFSET (7) NULL,
    [is_retired]             BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [version_number]         INT                NOT NULL,
    [updated_by]             VARCHAR (128)      NULL,
    [updated_datetime]       DATETIMEOFFSET (7) NULL,
    [release_number]         INT                NULL
);

