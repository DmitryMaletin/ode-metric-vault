CREATE PROCEDURE [stage].[usp_Link_Integrity]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

EXEC [$(ODE_Config)].dv_integrity.dv_link_metrics 0,'ODE_Metrics_Stage','Stage','Link_Integrity'

END