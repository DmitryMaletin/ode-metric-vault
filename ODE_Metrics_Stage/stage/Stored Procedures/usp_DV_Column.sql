
CREATE PROCEDURE [stage].[usp_DV_Column]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Column'))
	DROP TABLE stage.DV_Column;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,c.[column_key]
		,c.[table_key]
		,c.[satellite_col_key]
		,c.[column_name]
		,c.[column_type]
		,c.[column_length]
		,c.[column_precision]
		,c.[column_scale]
		,c.[Collation_Name]
		,c.[is_derived]
		,c.[derived_value]
		,c.[source_ordinal_position]
		,c.[is_source_date]
		,c.[is_retired]
		,c.[release_key]
		,m.[release_number]
		,c.[version_number]
		,c.[updated_by]
		,c.[update_date_time]
	INTO [stage].[DV_Column]
	FROM [$(ODE_Config)].[dbo].[dv_column] c
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON c.release_key = m.release_key
END