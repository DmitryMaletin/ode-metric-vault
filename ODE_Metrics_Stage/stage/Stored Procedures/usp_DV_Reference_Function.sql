
CREATE PROCEDURE [stage].[usp_DV_Reference_Function]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Reference_Function'))
	DROP TABLE stage.DV_Reference_Function;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,f.[ref_function_key]
      ,f.[ref_function_name]
      ,f.[ref_function]
      ,f.[is_retired]
      ,f.[release_key]
	  ,m.release_number
      ,f.[version_number]
      ,f.[updated_by]
      ,f.[updated_datetime]
	INTO [stage].[DV_Reference_Function]
	FROM [$(ODE_Config)].[dbo].[dv_ref_function] f
	LEFT JOIN [$(ODE_Config)].[dv_release].dv_release_master m
	ON f.release_key = m.release_key
END