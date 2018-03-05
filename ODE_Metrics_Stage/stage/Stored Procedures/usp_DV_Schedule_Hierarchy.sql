CREATE PROCEDURE [stage].[usp_DV_Schedule_Hierarchy]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Schedule_Hierarchy'))
	DROP TABLE stage.DV_Schedule_Hierarchy;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[source_table_hierarchy_key]
		,s.[source_table_key]
		,s.[prior_table_key]
		,s.[is_cancelled]
		,s.[release_key]
		,m.[release_number]
		,s.[version_number]
		,s.[updated_by]
		,s.[update_date_time]
	INTO [stage].[DV_Schedule_Hierarchy]
	FROM [$(ODE_Config)].[dv_scheduler].[dv_source_table_hierarchy] s
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON s.release_key = m.release_key
END