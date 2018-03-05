CREATE TABLE [stage].[Link_Manifest_Source] (
    [metrics_stage_run_time] DATETIMEOFFSET (7) NOT NULL,
    [source_table_key]       INT                NULL,
    [run_manifest_key]       INT                NULL
);

