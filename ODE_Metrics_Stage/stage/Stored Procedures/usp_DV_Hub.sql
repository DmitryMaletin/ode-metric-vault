
--TO DO: Config database variable
--TO DO: Depending on the delta switch option, al stored procedures should have or have not a parameter

CREATE PROCEDURE [stage].[usp_DV_Hub]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Hub'))
	DROP TABLE stage.DV_Hub;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,h.[hub_key]
		,h.[hub_name]
		,h.[hub_abbreviation]
		,h.[hub_schema]
		,h.[hub_database]
		,h.[is_retired]
		,h.[is_compressed]
		,h.[release_key]
		,m.[release_number]
		,h.[version_number]
		,h.[updated_by]
		,h.[updated_datetime]
	INTO [stage].[DV_Hub]
	FROM [$(ODE_Config)].[dbo].[dv_hub] h
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON h.release_key = m.release_key
END