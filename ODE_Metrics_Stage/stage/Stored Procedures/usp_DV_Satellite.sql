
CREATE PROCEDURE [stage].[usp_DV_Satellite]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Satellite'))
	DROP TABLE stage.DV_Satellite;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[satellite_key]
		,s.[hub_key]
		,s.[link_key]
		,s.[link_hub_satellite_flag]
		,s.[satellite_name]
		,s.[satellite_abbreviation]
		,s.[satellite_schema]
		,s.[satellite_database]
		,s.[duplicate_removal_threshold]
		,s.[is_columnstore]
		,s.[is_compressed]
		,s.[is_retired]
		,s.[release_key]
		,m.[release_number]
		,s.[version_number]
		,s.[updated_by]
		,s.[updated_datetime]
	INTO [stage].[DV_Satellite]
	FROM [$(ODE_Config)].[dbo].[dv_satellite] s
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON s.release_key = m.release_key
END