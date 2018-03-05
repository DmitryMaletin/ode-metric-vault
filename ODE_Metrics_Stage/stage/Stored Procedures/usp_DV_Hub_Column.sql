CREATE PROCEDURE [stage].[usp_DV_Hub_Column]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Hub_Column'))
	DROP TABLE stage.DV_Hub_Column;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,c.[hub_col_key]
		,c.[hub_key_column_key]
		,c.link_key_column_key
		,c.[column_key]
		,c.[release_key]
		,m.[release_number]
		,c.[version_number]
		,c.[updated_by]
		,c.[updated_datetime]
	INTO [stage].[DV_Hub_Column]
	FROM [$(ODE_Config)].[dbo].[dv_hub_column] c
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON c.release_key = m.release_key
END