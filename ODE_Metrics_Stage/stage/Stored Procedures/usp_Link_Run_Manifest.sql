CREATE PROCEDURE [stage].[usp_Link_Run_Manifest]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Run_Manifest'))
	DROP TABLE stage.Link_Run_Manifest;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,run_key
		,run_manifest_key
	INTO [stage].[Link_Run_Manifest]
	FROM [ODE_Metrics_Vault].[Sat].[s_DV_Run_Manifest]
	WHERE [dv_row_is_current] = 1
	AND [dv_is_tombstone] = 0
END