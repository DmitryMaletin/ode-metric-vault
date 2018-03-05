CREATE TABLE [stage].[DV_Schedule_Hierarchy] (
    [metrics_stage_run_time]     DATETIMEOFFSET (7) NOT NULL,
    [source_table_hierarchy_key] INT                NOT NULL,
    [source_table_key]           INT                NOT NULL,
    [prior_table_key]            INT                NOT NULL,
    [is_cancelled]               BIT                NOT NULL,
    [release_key]                INT                NOT NULL,
    [release_number]             INT                NULL,
    [version_number]             INT                NOT NULL,
    [updated_by]                 VARCHAR (128)       NOT NULL,
    [update_date_time]           DATETIMEOFFSET (7) NOT NULL
);

