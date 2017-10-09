CREATE TABLE [Stage].[DV_Column_Match] (
    [metrics_stage_run_time]    DATETIMEOFFSET (7) NOT NULL,
    [col_match_key]             INT                NOT NULL,
    [match_key]                 INT                NOT NULL,
    [left_hub_key_column_key]   INT                NULL,
    [left_link_key_column_key]  INT                NULL,
    [left_satellite_col_key]    INT                NULL,
    [left_column_key]           INT                NULL,
    [right_hub_key_column_key]  INT                NULL,
    [right_link_key_column_key] INT                NULL,
    [right_satellite_col_key]   INT                NULL,
    [right_column_key]          INT                NULL,
    [release_key]               INT                NOT NULL,
    [version_number]            INT                NOT NULL,
    [updated_by]                VARCHAR (128)      NULL,
    [updated_datetime]          DATETIMEOFFSET (7) NULL,
    [release_number]            INT                NULL
);

