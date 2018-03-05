

CREATE PROCEDURE [Stage].[usp_DV_Column_Match]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Column_Match'))
	DROP TABLE stage.DV_Column_Match;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,c.[col_match_key]
      ,c.[match_key]
      ,c.[left_hub_key_column_key]
      ,c.[left_link_key_column_key]
      ,c.[left_satellite_col_key]
      ,c.[left_column_key]
      ,c.[right_hub_key_column_key]
      ,c.[right_link_key_column_key]
      ,c.[right_satellite_col_key]
      ,c.[right_column_key]
      ,c.[release_key]
      ,c.[version_number]
      ,c.[updated_by]
      ,c.[updated_datetime]
		,m.[release_number]
	INTO [stage].[DV_Column_Match]
	FROM [$(ODE_Config)].[dbo].[dv_column_match] c
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON c.release_key = m.release_key
END