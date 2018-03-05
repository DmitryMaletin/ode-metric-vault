CREATE TABLE [stage].[DV_Reference_Function] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [ref_function_key]       INT                NOT NULL,
    [ref_function_name]      VARCHAR (128)      NOT NULL,
    [ref_function]           NVARCHAR (4000)    NOT NULL,
    [is_retired]             BIT                NOT NULL,
    [release_key]            INT                NOT NULL,
    [release_number]         INT                NULL,
    [version_number]         INT                NOT NULL,
    [updated_by]             VARCHAR (128)       NULL,
    [updated_datetime]       DATETIMEOFFSET (7) NULL
);

