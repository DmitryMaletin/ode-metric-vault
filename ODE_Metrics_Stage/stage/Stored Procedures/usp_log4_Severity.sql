

--TO DO: Config database variable
--TO DO: Depending on the delta switch option, al stored procedures should have or have not a parameter

CREATE PROCEDURE [stage].[usp_log4_Severity]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'log4_Severity'))
	DROP TABLE stage.log4_Severity;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,s.[SeverityId]
		,s.[SeverityName]
	INTO [stage].[log4_Severity]
	FROM [$(ODE_Config)].[log4].[Severity] s

END