CREATE TABLE [stage].[DV_Link_Key_Column] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [link_key_column_key]    INT                NOT NULL,
    [link_key]               INT                NOT NULL,
    [link_key_column_name]   VARCHAR (128)      NOT NULL,
    [release_key]            INT                NOT NULL,
    [release_number]         INT                NULL,
    [version_number]         INT                NOT NULL,
    [updated_by]             VARCHAR (128)       NULL,
    [updated_datetime]       DATETIMEOFFSET (7) NULL
);



