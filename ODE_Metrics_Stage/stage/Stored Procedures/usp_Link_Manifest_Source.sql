CREATE PROCEDURE [stage].[usp_Link_Manifest_Source]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Manifest_Source'))
	DROP TABLE stage.Link_Manifest_Source;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,source_table_key
		,run_manifest_key
	INTO [stage].[Link_Manifest_Source]
	FROM [ODE_Metrics_Vault].[Sat].[s_DV_Run_Manifest]
	WHERE [dv_row_is_current] = 1
	AND [dv_is_tombstone] = 0
END