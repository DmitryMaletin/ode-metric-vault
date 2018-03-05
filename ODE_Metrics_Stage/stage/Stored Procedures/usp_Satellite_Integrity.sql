
CREATE PROCEDURE [stage].[usp_Satellite_Integrity]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

EXEC [$(ODE_Config)].dv_integrity.dv_sat_metrics 0,'ODE_Metrics_Stage','Stage','Satellite_Integrity'

END