CREATE PROCEDURE [stage].[usp_DV_Exception]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Exception'))
	DROP TABLE stage.DV_Exception;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,[ExceptionId]
		,[UtcDate]
		,[SystemDate]
		,[ErrorContext]
		,[ErrorNumber]
		,[ErrorSeverity]
		,[ErrorState]
		,[ErrorProcedure]
		,[ErrorLine]
		,[ErrorMessage]
		,[SessionId]
		,[ServerName]
		,[DatabaseName]
		,[HostName]
		,[ProgramName]
		,[NTDomain]
		,[NTUsername]
		,[LoginName]
		,[OriginalLoginName]
		,[SessionLoginTime]
	INTO [stage].[DV_Exception]
	FROM [$(ODE_Config)].[log4].[Exception] e
END