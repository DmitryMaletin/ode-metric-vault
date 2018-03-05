CREATE PROCEDURE [stage].[usp_Link_Hub_Satellite]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Hub_Satellite'))
	DROP TABLE stage.Link_Hub_Satellite;

		SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,satellite_key
		, hub_key
	INTO [stage].[Link_Hub_Satellite]
	FROM [ODE_Metrics_Vault].[Sat].[s_DV_Satellite]
	WHERE link_hub_satellite_flag = 'H'
	AND [dv_row_is_current] = 1
	AND [dv_is_tombstone] = 0
END