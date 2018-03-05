
CREATE PROCEDURE [stage].[usp_DV_Link]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Link'))
	DROP TABLE stage.DV_Link;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,l.[link_key]
		,l.[link_name]
		,l.[link_abbreviation]
		,l.[link_schema]
		,l.[link_database]
		,l.[is_compressed]
		,l.[is_retired]
		,l.[release_key]
		,m.[release_number]
		,l.[version_number]
		,l.[updated_by]
		,l.[updated_datetime]
	INTO [stage].[DV_Link]
	FROM [$(ODE_Config)].[dbo].[dv_link] l
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON l.release_key = m.release_key
END