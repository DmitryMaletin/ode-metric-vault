CREATE PROCEDURE [stage].[usp_DV_Schedule]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Schedule'))
	DROP TABLE stage.DV_Schedule;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[schedule_key]
		,s.[schedule_name]
		,s.[schedule_description]
		,s.[schedule_frequency]
		,s.[is_cancelled]
		,s.[release_key]
		,m.[release_number]
		,s.[version_number]
		,s.[updated_by]
		,s.[updated_datetime]
	INTO [stage].[DV_Schedule]
	FROM [$(ODE_Config)].[dv_scheduler].[dv_schedule] s
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON s.release_key = m.release_key
END