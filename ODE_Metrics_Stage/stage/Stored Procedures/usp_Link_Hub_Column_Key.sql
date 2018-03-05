CREATE PROCEDURE [stage].[usp_Link_Hub_Column_Key]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Hub_Column_Key'))
	DROP TABLE stage.Link_Hub_Column_Key;

	;WITH hHubKey	AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Hub_Key])
	,sColumn		AS (SELECT * FROM [ODE_Metrics_Vault].[RawSat].[s_DV_Hub_Column]	WHERE [dv_row_is_current] = 1	AND [dv_is_tombstone] = 0)
	,sHubKey		AS (SELECT * FROM [ODE_Metrics_Vault].[Sat].[s_DV_Hub_Key]		WHERE [dv_row_is_current] = 1	AND [dv_is_tombstone] = 0)

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,sHubKey.hub_key
		,k.hub_key_column_key
		,c.column_key
	INTO [stage].[Link_Hub_Column_Key]
	FROM hHubKey k
	JOIN sHubKey ON k.h_DV_Hub_Key_key = sHubKey.h_DV_Hub_Key_key
	JOIN sColumn c ON c.hub_key_column_key = k.hub_key_column_key
END