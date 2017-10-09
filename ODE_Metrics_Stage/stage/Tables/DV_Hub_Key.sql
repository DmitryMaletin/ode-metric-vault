CREATE TABLE [stage].[DV_Hub_Key] (
    [metrics_stage_run_time]   DATETIMEOFFSET (7) NOT NULL,
    [hub_key_column_key]       INT                NOT NULL,
    [hub_key]                  INT                NOT NULL,
    [hub_key_column_name]      VARCHAR (128)      NOT NULL,
    [hub_key_column_type]      VARCHAR (30)       NOT NULL,
    [hub_key_column_length]    INT                NULL,
    [hub_key_column_precision] INT                NULL,
    [hub_key_column_scale]     INT                NULL,
    [hub_key_Collation_Name]   [sysname]          NULL,
    [hub_key_ordinal_position] INT                NOT NULL,
    [release_key]              INT                NOT NULL,
    [release_number]           INT                NULL,
    [version_number]           INT                NOT NULL,
    [updated_by]               VARCHAR (128)       NULL,
    [updated_datetime]         DATETIMEOFFSET (7) NULL
);

