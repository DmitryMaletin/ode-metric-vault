CREATE PROCEDURE [stage].[usp_DV_Schedule_Table]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Schedule_Table'))
	DROP TABLE stage.DV_Schedule_Table;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[schedule_source_table_key]
		,s.[schedule_key]
		,s.[source_table_key]
		,s.[source_table_load_type]
		,s.[priority]
		,s.[queue]
		,s.[is_cancelled]
		,s.[release_key]
		,m.[release_number]
		,s.[version_number]
		,s.[updated_by]
		,s.[updated_datetime]
	INTO [stage].[DV_Schedule_Table]
	FROM [$(ODE_Config)].[dv_scheduler].[dv_schedule_source_table] s
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON s.release_key = m.release_key
END