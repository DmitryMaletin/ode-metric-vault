
CREATE PROCEDURE [stage].[usp_DV_Hub_Key]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Hub_Key'))
	DROP TABLE stage.DV_Hub_Key;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,c.[hub_key_column_key]
		,c.[hub_key]
		,c.[hub_key_column_name]
		,c.[hub_key_column_type]
		,c.[hub_key_column_length]
		,c.[hub_key_column_precision]
		,c.[hub_key_column_scale]
		,c.[hub_key_Collation_Name]
		,c.[hub_key_ordinal_position]
		,c.[release_key]
		,m.[release_number]
		,c.[version_number]
		,c.[updated_by]
		,c.[updated_datetime]
	INTO [stage].[DV_Hub_Key]
	FROM [$(ODE_Config)].[dbo].[dv_hub_key_column] c
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON c.release_key = m.release_key
END