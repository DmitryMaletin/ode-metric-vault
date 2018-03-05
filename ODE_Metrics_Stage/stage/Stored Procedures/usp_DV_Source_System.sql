CREATE PROCEDURE [stage].[usp_DV_Source_System]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Source_System'))
	DROP TABLE stage.DV_Source_System;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[source_system_key]
		,s.[source_system_name]
		,s.[source_database_name]
		,s.[package_folder]
		,s.[package_project]
		,s.[project_connection_name]
		,s.[is_retired]
		,s.[release_key]
		,m.[release_number]
		,s.[version_number]
		,s.[updated_by]
		,s.[update_date_time]
	INTO [stage].[DV_Source_System]
	FROM [$(ODE_Config)].[dbo].[dv_source_system] s
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON s.release_key = m.release_key
END