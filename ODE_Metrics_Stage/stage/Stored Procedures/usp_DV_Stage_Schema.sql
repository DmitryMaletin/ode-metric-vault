

CREATE PROCEDURE [Stage].[usp_DV_Stage_Schema]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Stage_Schema'))
	DROP TABLE stage.DV_Stage_Schema;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[stage_schema_key]
      ,s.[stage_database_key]
      ,s.[stage_schema_name]
      ,s.[is_retired]
      ,s.[release_key]
      ,s.[version_number]
      ,s.[updated_by]
      ,s.[update_date_time]
		,m.[release_number]
	INTO [stage].[DV_Stage_Schema]
	FROM [$(ODE_Config)].[dbo].[dv_stage_schema] s
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON s.release_key = m.release_key
END