
CREATE PROCEDURE [stage].[usp_Link_Column_Source]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Column_Source'))
	DROP TABLE stage.Link_Column_Source;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,table_key AS source_table_key
		,column_key
	INTO [stage].[Link_Column_Source]
	FROM [ODE_Metrics_Vault].[Sat].[s_DV_Column]
	WHERE [dv_row_is_current] = 1
	AND [dv_is_tombstone] = 0
END