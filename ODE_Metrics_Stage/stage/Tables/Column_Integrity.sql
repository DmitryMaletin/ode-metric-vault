CREATE TABLE [stage].[Column_Integrity] (
    [runtime]      VARCHAR (34)  NOT NULL,
    [sat_key]      INT           NOT NULL,
    [sat_name]     VARCHAR (128) NOT NULL,
    [column_name]  VARCHAR (128) NOT NULL,
    [column_key]   VARCHAR (128) NOT NULL,
    [min_value]    VARCHAR (MAX) NULL,
    [max_value]    VARCHAR (MAX) NULL,
    [domain_count] BIGINT        NULL,
    [null_count]   BIGINT        NULL,
    [blank_count]  BIGINT        NULL,
    [minlength]    BIGINT        NULL,
    [maxlength]    BIGINT        NULL
);





