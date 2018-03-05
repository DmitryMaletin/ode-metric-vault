

CREATE PROCEDURE [Stage].[usp_DV_Source_Table]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Source_Table'))
	DROP TABLE stage.DV_Source_Table;

;WITH hSTable	AS (SELECT * FROM [ODE_Metrics_Vault].hub.h_DV_Source_Table)
,hSSys			AS (SELECT * FROM [ODE_Metrics_Vault].RawHub.h_DV_Source_System)
,sSTable		AS (SELECT * FROM [ODE_Metrics_Vault].RawSat.s_DV_Source_Table_Raw WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,sSSys			AS (SELECT * FROM [ODE_Metrics_Vault].RawSat.s_DV_Source_System WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,hSTable.[source_table_key]
		,sSTable.[system_key] AS [source_system_key]
		,sSSys.[is_retired] AS [source_system_is_retired]
		,sSSys.[source_system_name]
		,sSSys.[release_key] AS [source_system_release_key]
		,sSSys.[release_number] AS [source_system_release_number]
		,sSTable.[source_table_schema]
		,sSTable.[source_table_name]
		,sSTable.source_unique_name
		,sSTable.[source_table_load_type]
		,sSTable.[is_retired]
		,sSTable.[release_key]
		,sSTable.[release_number]
		,sSTable.[updated_by]
		,sSTable.[update_date_time]
	INTO [stage].[DV_Source_Table]
	FROM hSTable
	LEFT JOIN sSTable	ON hSTable.h_DV_Source_Table_key = sSTable.h_DV_Source_Table_key
	LEFT JOIN hSSys		ON sSTable.system_key = hSSys.source_system_key
	LEFT JOIN sSSys		ON hSSys.h_DV_Source_System_key = sSSys.h_DV_Source_System_key
END