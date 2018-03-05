CREATE PROCEDURE [stage].[usp_Link_Hub_Link_Column]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Hub_Link_Column'))
	DROP TABLE stage.Link_Hub_Link_Column;

	with hHC	AS (SELECT * FROM [ODE_Metrics_Vault].[RawHub].[h_DV_Hub_Column])
	, sHC		AS (SELECT * FROM [ODE_Metrics_Vault].[RawSat].[s_DV_Hub_Column]
					WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] =0)

	SELECT DISTINCT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,ISNULL(sHC.[link_key_column_key],-999999) [link_key_column_key]
		,sHC.[hub_key_column_key]
		,sHC.[column_key]
	INTO [stage].[Link_Hub_Link_Column]
	FROM hHC
	JOIN sHC
	ON hHC.h_DV_Hub_Column_key = sHC.h_DV_Hub_Column_key
END