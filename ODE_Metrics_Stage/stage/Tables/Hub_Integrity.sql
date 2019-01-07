CREATE TABLE [stage].[Hub_Integrity] (
    [RunDate]          DATETIMEOFFSET (7) NULL,
    [HubKey]           INT                NULL,
    [HubName]          VARCHAR (128)      NULL,
    [SourceVersionKey] INT                NULL,
    [SourceSystemName] VARCHAR (128)      NULL,
    [SourceTableName]  VARCHAR (128)      NULL,
    [TotalRowCount]    BIGINT             NULL
);

