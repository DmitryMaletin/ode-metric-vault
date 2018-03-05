CREATE PROCEDURE [stage].[usp_DV_Journal]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

	IF (EXISTS (SELECT * 
				FROM INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_SCHEMA = 'stage' AND TABLE_NAME = 'DV_Journal'))
	DROP TABLE stage.DV_Journal;

	SELECT metrics_stage_run_time = SYSDATETIMEOFFSET()
		,j.[JournalId]
		,j.[UtcDate]
		,j.[SystemDate]
		,j.[Task]
		,j.[FunctionName]
		,j.[StepInFunction]
		,j.[MessageText]
		,j.[SeverityId]
		,j.[ExceptionId]
		,j.[SessionId]
		,j.[ServerName]
		,j.[DatabaseName]
		,j.[HostName]
		,j.[ProgramName]
		,j.[NTDomain]
		,j.[NTUsername]
		,j.[LoginName]
		,j.[OriginalLoginName]
		,j.[SessionLoginTime]
		,jd.[ExtraInfo]
	INTO [stage].[DV_Journal]
	FROM [$(ODE_Config)].[log4].[Journal] j
	LEFT JOIN [$(ODE_Config)].[log4].[JournalDetail] jd
	ON j.JournalId = jd.JournalId
END