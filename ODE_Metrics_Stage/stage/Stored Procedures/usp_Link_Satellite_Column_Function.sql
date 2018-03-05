
CREATE PROCEDURE [stage].[usp_Link_Satellite_Column_Function]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Satellite_Column_Function'))
	DROP TABLE stage.Link_Satellite_Column_Function;

	WITH sSatCol AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Satellite_Column] WHERE dv_is_tombstone = 0 AND dv_row_is_current = 1)
	
	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.satellite_col_key
      ,s.ref_function_key
	INTO [stage].Link_Satellite_Column_Function
	FROM sSatCol s
	WHERE s.ref_function_key IS NOT NULL
	
END