CREATE TABLE [stage].[DV_Satellite] (
    [metrics_stage_run_time]      DATETIMEOFFSET (7) NOT NULL,
    [satellite_key]               INT                NOT NULL,
    [hub_key]                     INT                NOT NULL,
    [link_key]                    INT                NOT NULL,
    [link_hub_satellite_flag]     CHAR (1)           NOT NULL,
    [satellite_name]              VARCHAR (128)      NOT NULL,
    [satellite_abbreviation]      VARCHAR (4)        NULL,
    [satellite_schema]            VARCHAR (128)      NOT NULL,
    [satellite_database]          VARCHAR (128)      NOT NULL,
    [duplicate_removal_threshold] INT                NOT NULL,
    [is_columnstore]              BIT                NOT NULL,
    [is_compressed]               BIT                NOT NULL,
    [is_retired]                  BIT                NOT NULL,
    [release_key]                 INT                NOT NULL,
    [release_number]              INT                NULL,
    [version_number]              INT                NOT NULL,
    [updated_by]                  VARCHAR (128)       NULL,
    [updated_datetime]            DATETIMEOFFSET (7) NULL
);



