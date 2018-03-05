CREATE PROCEDURE [stage].[usp_Link_Link_Key]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'Link_Link_Key'))
	DROP TABLE stage.Link_Link_Key;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,[link_key_column_key]
		,link_key
	INTO [stage].[Link_Link_Key]
	FROM [$(ODE_Config)].[dbo].[dv_link_key_column]

END