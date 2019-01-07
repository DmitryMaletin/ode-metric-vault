CREATE TABLE [stage].[Link_Integrity] (
    [RunDate]          DATETIMEOFFSET (7) NULL,
    [LinkKey]          INT                NULL,
    [LinkName]         VARCHAR (128)      NULL,
    [SourceVersionKey] INT                NULL,
    [SourceSystemName] VARCHAR (128)      NULL,
    [SourceTableName]  VARCHAR (128)      NULL,
    [TotalRowCount]    BIGINT             NULL
);

