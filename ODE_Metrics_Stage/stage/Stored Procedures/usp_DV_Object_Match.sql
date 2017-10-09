

CREATE PROCEDURE [Stage].[usp_DV_Object_Match]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Object_Match'))
	DROP TABLE stage.DV_Object_Match;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,c.[match_key]
      ,c.[source_version_key]
      ,c.[temporal_pit_left]
      ,c.[temporal_pit_right]
      ,c.[is_retired]
      ,c.[release_key]
      ,c.[version_number]
      ,c.[updated_by]
      ,c.[updated_datetime]
		,m.[release_number]
	INTO [stage].[DV_Object_Match]
	FROM [$(ODE_Config)].[dbo].[dv_object_match] c
	LEFT JOIN [$(ODE_Config)].[dv_release].[dv_release_master] m
	ON c.release_key = m.release_key
END