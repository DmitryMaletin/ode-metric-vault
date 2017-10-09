
CREATE PROCEDURE [stage].[usp_DV_Release]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Release'))
	DROP TABLE stage.DV_Release;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,[release_number]
		,[release_key]
		,[release_description]
		,[reference_number]
		,[reference_source]
		,[build_number]
		,[build_date]
		,[build_server]
		,[release_built_by]
		,[release_start_datetime]
		,[release_complete_datetime]
		,[release_count]
		,[version_number]
		,[updated_by]
		,[updated_datetime]
	INTO [stage].[DV_Release]
	FROM [$(ODE_Config)].[dv_release].[dv_release_master] m
END