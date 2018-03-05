
CREATE PROCEDURE [stage].[usp_DV_Schedule_Run]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Schedule_Run'))
	DROP TABLE stage.DV_Schedule_Run;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,[run_key]
		,[run_status]
		,[run_schedule_name]
		,[run_start_datetime]
		,[run_end_datetime]
		,[updated_datetime]
	INTO [stage].[DV_Schedule_Run]
	FROM [$(ODE_Config)].[dv_scheduler].[dv_run]
END