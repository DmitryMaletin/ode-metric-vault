CREATE TABLE [stage].[DV_Satellite_Column] (
    [metrics_stage_run_time]     DATETIMEOFFSET (7) NOT NULL,
    [satellite_col_key]          INT                NOT NULL,
    [satellite_key]              INT                NOT NULL,
    [column_name]                VARCHAR (128)      NOT NULL,
    [column_type]                VARCHAR (30)       NOT NULL,
    [column_length]              INT                NULL,
    [column_precision]           INT                NULL,
    [column_scale]               INT                NULL,
    [collation_name]             [sysname]          NULL,
    [satellite_ordinal_position] INT                NOT NULL,
    [ref_function_key]           INT                NULL,
    [func_arguments]             NVARCHAR (512)     NULL,
    [func_ordinal_position]      INT                NOT NULL,
    [release_key]                INT                NOT NULL,
    [release_number]             INT                NULL,
    [version_number]             INT                NOT NULL,
    [updated_by]                 VARCHAR (128)       NULL,
    [updated_datetime]           DATETIMEOFFSET (7) NULL
);



