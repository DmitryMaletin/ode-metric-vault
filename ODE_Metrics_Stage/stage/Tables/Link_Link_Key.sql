CREATE TABLE [stage].[Link_Link_Key] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [link_key_column_key]    INT                IDENTITY (1, 1) NOT NULL,
    [link_key]               INT                NOT NULL
);

