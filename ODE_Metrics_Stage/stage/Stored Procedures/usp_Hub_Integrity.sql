CREATE PROCEDURE [stage].[usp_Hub_Integrity]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

EXEC [$(ODE_Config)].dv_integrity.dv_hub_metrics 0,'ODE_Metrics_Stage','Stage','Hub_Integrity'

END