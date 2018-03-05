
CREATE PROCEDURE [stage].[usp_DV_Run_Manifest]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Run_Manifest'))
	DROP TABLE stage.DV_Run_Manifest;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,[run_manifest_key]
		,[run_key]
		,[source_unique_name]
		,[source_table_load_type]
		,[source_table_key]
		,[priority]
		,[queue]
		,[start_datetime]
		,[completed_datetime]
		,[run_status]
		,[row_count]
		,[session_id]
	INTO [stage].[DV_Run_Manifest]
	FROM [$(ODE_Config)].[dv_scheduler].[dv_run_manifest]
END