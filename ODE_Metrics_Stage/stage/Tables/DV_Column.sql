CREATE TABLE [stage].[DV_Column] (
    [metrics_stage_run_time]  DATETIMEOFFSET (7) NOT NULL,
    [column_key]              INT                NOT NULL,
    [table_key]               INT                NOT NULL,
    [satellite_col_key]       INT                NULL,
    [column_name]             VARCHAR (128)      NOT NULL,
    [column_type]             VARCHAR (30)       NOT NULL,
    [column_length]           INT                NULL,
    [column_precision]        INT                NULL,
    [column_scale]            INT                NULL,
    [Collation_Name]          [sysname]          NULL,
    [is_derived]              BIT                NULL,
    [derived_value]           VARCHAR (50)       NULL,
    [source_ordinal_position] INT                NOT NULL,
    [is_source_date]          BIT                NOT NULL,
    [is_retired]              BIT                NOT NULL,
    [release_key]             INT                NOT NULL,
    [release_number]          INT                NULL,
    [version_number]          INT                NOT NULL,
    [updated_by]              VARCHAR (128)       NULL,
    [update_date_time]        DATETIMEOFFSET (7) NULL
);



