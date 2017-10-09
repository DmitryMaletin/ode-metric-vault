CREATE TABLE [stage].[DV_Source_System] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [source_system_key]      INT                NOT NULL,
    [source_system_name]     VARCHAR (50)       NOT NULL,
    [is_retired]             BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [release_number]         INT                NULL,
    [version_number]         INT                NULL,
    [updated_by]             VARCHAR (128)       NULL,
    [update_date_time]       DATETIMEOFFSET (7) NULL
);



