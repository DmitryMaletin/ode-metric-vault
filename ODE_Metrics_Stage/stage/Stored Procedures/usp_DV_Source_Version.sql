

CREATE PROCEDURE [Stage].[usp_DV_Source_Version]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Source_Version'))
	DROP TABLE stage.DV_Source_Version;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[source_version_key]
      ,s.[source_table_key]
      ,s.[source_version]
      ,s.[source_type]
      ,s.[source_procedure_name]
      ,s.[source_filter]
      ,s.[pass_load_type_to_proc]
      ,s.[is_current]
      ,s.[release_key]
      ,s.[version_number]
      ,s.[updated_by]
      ,s.[update_date_time]
		,m.[release_number]
	INTO [stage].[DV_Source_Version]
	FROM [$(ODE_Config)].[dbo].[dv_source_version] s
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON s.release_key = m.release_key
END