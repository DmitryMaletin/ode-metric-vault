﻿CREATE PROCEDURE [stage].[usp_Column_Integrity]
--	@LoadType varchar(128)
AS
BEGIN
	SET NOCOUNT ON;

EXEC [$(ODE_Config)].dv_integrity.dv_col_metrics 0, [$(DatabaseName)],'Stage','Column_Integrity'

END