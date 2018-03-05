CREATE PROCEDURE [stage].[usp_DV_Link_Key_Column]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Link_Key_Column'))
	DROP TABLE stage.DV_Link_Key_Column;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,l.[link_key_column_key]
		,l.[link_key]
		,l.[link_key_column_name]
		,l.[release_key]
		,m.[release_number]
		,l.[version_number]
		,l.[updated_by]
		,l.[updated_datetime]
	INTO [stage].[DV_Link_Key_Column]
	FROM [$(ODE_Config)].[dbo].[DV_Link_Key_Column] l
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON l.release_key = m.release_key
END