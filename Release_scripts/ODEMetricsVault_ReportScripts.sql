
/*
	ODE Metrics Vault aggregating views.
	Use them to access the Metrics Vault data or create your own.
*/

------------------------------------------------------------------
--Data dictionary for columns
--Use this view to show and update the data dictionary.
--Data dictionary information is not written directly to the Metrics Vault, 
--   but it will be available after the next Metrics Vault schedule run

CREATE VIEW [dbo].[vw_DD_Columns]
AS

WITH sDDColumn		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_Column_DataDictionary] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,hSat			AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Satellite])
,sSat			AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Satellite])
,sColInt		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_Satellite_Column_Integrity] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,hSatCol		AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Satellite_Column])
,sSatCol		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Satellite_Column] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,lSatCol		AS (SELECT l.* FROM [ODE_Metrics_Vault].lnk.l_Satellite_column_Satellite l JOIN [ODE_Metrics_Vault].sat.s_Link_Satellite_Column_Satellite s
					ON s.l_Satellite_Column_Satellite_key = l.l_Satellite_column_Satellite_key WHERE dv_is_tombstone = 0 and dv_row_is_current = 1)

SELECT DISTINCT 
  sSat.satellite_name		AS SatelliteName
, CASE WHEN sSat.is_retired = 0 THEN 'Current' WHEN sSat.is_retired IS NULL THEN 'N/A' Else 'Retired' END AS SatelliteCurrentFlag
, hSatCol.satellite_col_key		AS SatelliteColumnKey
, sSatCol.column_name		AS ColumnName
, sSatCol.column_type		AS ColumnType
, sSatCol.column_length		AS ColumnLength
, sDDColumn.[Description]	AS ColumnShortDesc
, sDDColumn.[BusinessRule]	AS ColumnLongDesc
, sColInt.[MinValue]
, sColInt.[MaxValue]
, sColInt.[DomainCount]
, sColInt.[NullCount]
, sColInt.[BlankCount]
, sColInt.[MinLength]
, sColInt.[MaxLength]
FROM hSatCol
JOIN sSatCol			ON hSatCol.h_DV_Satellite_Column_key = sSatCol.h_DV_Satellite_Column_key
LEFT JOIN sDDColumn		ON hSatCol.h_DV_Satellite_Column_key = sDDColumn.h_DV_Satellite_Column_key
LEFT JOIN lSatCol		on hSatCol.h_DV_Satellite_Column_key = lSatCol.h_DV_Satellite_Column_key
LEFT JOIN hSat			ON lSatCol.h_DV_Satellite_key = hSat.h_DV_Satellite_key
LEFT JOIN sSat			ON hSat.h_DV_Satellite_key = sSat.h_DV_Satellite_key
LEFT JOIN sColInt		ON sColInt.h_DV_Satellite_Column_key = hSatCol.h_DV_Satellite_Column_key

GO


Create TRIGGER [dbo].[DD_Columns_InsDel_Trg] on [dbo].[vw_DD_Columns]
INSTEAD OF INSERT, DELETE
AS
BEGIN
Print 'Explisit inserts and deletions on Data Dictionary tables are not allowed'
END

GO


CREATE TRIGGER [dbo].[DD_Columns_Upd_Trg] on [dbo].[vw_DD_Columns]
INSTEAD OF UPDATE
AS
BEGIN

DECLARE @UpdateType char(1) = 'I'
DECLARE @ColumnKey int

DECLARE curUpdate CURSOR FOR
	SELECT SatelliteColumnKey
	FROM inserted
OPEN curUpdate
FETCH NEXT
FROM curUpdate
INTO @ColumnKey

WHILE @@FETCH_STATUS = 0
BEGIN

	SELECT @UpdateType = CASE WHEN COUNT(*) > 0 THEN 'U' ELSE 'I' END 
	FROM [ODE_Metrics_Stage].[stage].[Column_DataDictionary] src
	JOIN INSERTED
	ON src.column_key = @ColumnKey


	IF @UpdateType = 'U'
		UPDATE [ODE_Metrics_Stage].[stage].[Column_DataDictionary]
		SET [Column_DataDictionary].[Description] = inserted.ColumnShortDesc
		, [Column_DataDictionary].[BusinessRule] = inserted.ColumnLongDesc
		FROM inserted
		JOIN [ODE_Metrics_Stage].[stage].[Column_DataDictionary] d
		ON d.column_key = inserted.SatelliteColumnKey
		WHERE inserted.SatelliteColumnKey = @ColumnKey
		AND d.column_key = @ColumnKey
	ELSE 
		INSERT [ODE_Metrics_Stage].[stage].[Column_DataDictionary]
		SELECT SatelliteColumnKey, ColumnShortDesc, ColumnLongDesc, GETDATE()
		FROM inserted WHERE SatelliteColumnKey = @ColumnKey


	FETCH NEXT 
	FROM curUpdate 
	INTO @ColumnKey
END
CLOSE curUpdate
DEALLOCATE curUpdate

END

GO

------------------------------------------------------------------
--Data dictionary for links
--Use this view to show and update the data dictionary.
--Data dictionary information is not written directly to the Metrics Vault, 
--   but it will be available after the next Metrics Vault schedule run


CREATE VIEW [dbo].[vw_DD_Links]
AS

WITH hLink   AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Link])
,sLink       AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Link]  WHERE [dv_row_is_current] = 1 and [dv_is_tombstone] = 0)
,sLinkInt    AS (SELECT LinkKey, SUM(TotalRowCount) AS TotalRowCount FROM [ODE_Metrics_Vault].[sat].[s_Link_Integrity] WHERE [dv_row_is_current] = 1 and [dv_is_tombstone] = 0
                           GROUP BY LinkKey)
,sDDLink     AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_Link_DataDictionary] WHERE [dv_row_is_current] = 1 and [dv_is_tombstone] = 0)
,sHubInt     AS (SELECT HubKey, SUM(TotalRowCount) AS TotalRowCount FROM [ODE_Metrics_Vault].[sat].[s_Hub_Integrity] WHERE [dv_row_is_current] = 1 and [dv_is_tombstone] = 0
                           GROUP BY HubKey)
,hRank       AS (select distinct t1.h_DV_Link_key, t3.h_DV_Hub_key, t4.hub_name, t5.TotalRowCount,
                           DENSE_RANK () OVER (PARTITION BY t1.h_DV_Link_key ORDER BY t1.h_DV_Link_key, t3.h_DV_Hub_key ) AS RankRank
                           from [lnk].[l_Link_Key] t1
                           join [lnk].[l_Hub_Link_Column] t2 on t1.h_DV_Link_Key_Column_key = t2.h_DV_Link_Key_Column_key
                           join [lnk].[l_Hub_Column_Key] t3 on t2.h_DV_Hub_Key_key = t3.h_DV_Hub_Key_key
                           join [sat].[s_DV_Hub] t4 on t3.h_DV_Hub_key = t4.h_DV_Hub_key
                           left join sHubInt t5 on t4.hub_key = t5.Hubkey
                          where t4.dv_row_is_current = 1 and t4.dv_is_tombstone = 0)
 
SELECT DISTINCT hLink.link_key   AS LinkKey
, sLink.link_name          AS LinkName
, sDDLink.[Description] AS LinkShortDesc
, sDDLink.BusinessRule     AS LinkLongDesc
, CASE WHEN sLink.is_retired = 0 THEN 'Current' ELSE 'Retired' END AS LinkCurrentFlag
, sLinkInt.TotalRowCount AS LinkRowCount
, h1.hub_name              AS Hub1Name
, h1.TotalRowCount         AS Hub1RowCount
, h2.hub_name              AS Hub2Name
, h2.TotalRowCount         AS Hub2RowCount
, h3.hub_name              AS Hub3Name
, h3.TotalRowCount         AS Hub3RowCount
, h4.hub_name              AS Hub4Name
, h4.TotalRowCount         AS Hub4RowCount
, h5.hub_name              AS Hub5Name
, h5.TotalRowCount         AS Hub5RowCount
, h6.hub_name              AS Hub6Name
, h6.TotalRowCount         AS Hub6RowCount
FROM hLink
LEFT JOIN hRank h1
       ON hLink.h_DV_Link_key = h1.h_DV_Link_key
       AND h1.RankRank = 1
LEFT JOIN hRank h2
       ON hLink.h_DV_Link_key = h2.h_DV_Link_key
       AND h2.RankRank = 2
LEFT JOIN hRank h3
       ON hLink.h_DV_Link_key = h3.h_DV_Link_key
       AND h3.RankRank = 3
LEFT JOIN hRank h4
       ON hLink.h_DV_Link_key = h4.h_DV_Link_key
       AND h4.RankRank = 4
LEFT JOIN hRank h5
       ON hLink.h_DV_Link_key = h5.h_DV_Link_key
       AND h5.RankRank = 5
LEFT JOIN hRank h6
       ON hLink.h_DV_Link_key = h6.h_DV_Link_key
       AND h6.RankRank = 6
LEFT JOIN sLink
       ON hLink.h_DV_Link_key = sLink.h_DV_Link_key
LEFT JOIN sLinkInt
       ON hLink.link_key = sLinkInt.LinkKey
LEFT JOIN sDDLink
       ON hLink.h_DV_Link_key = sDDLink.h_DV_Link_key
GO


Create TRIGGER [dbo].[DD_Links_InsDel_Trg] on [dbo].[vw_DD_Links]
INSTEAD OF INSERT, DELETE
AS
BEGIN
Print 'Explisit inserts and deletions on Data Dictionary tables are not allowed'
END
GO


CREATE TRIGGER [dbo].[DD_Links_Upd_Trg] on [dbo].[vw_DD_Links]
INSTEAD OF UPDATE
AS
BEGIN

DECLARE @UpdateType char(1) = 'I'
DECLARE @ColumnKey int

DECLARE curUpdate CURSOR FOR
	SELECT LinkKey
	FROM inserted
OPEN curUpdate
FETCH NEXT
FROM curUpdate
INTO @ColumnKey

WHILE @@FETCH_STATUS = 0
BEGIN

	SELECT @UpdateType = case when COUNT(*) > 0 THEN 'U' ELSE 'I' END 
	FROM [ODE_Metrics_Stage].[stage].[Link_DataDictionary] src
	JOIN INSERTED
	ON src.Link_key = @ColumnKey


	IF @UpdateType = 'U'
		UPDATE [ODE_Metrics_Stage].[stage].[Link_DataDictionary]
		SET [Link_DataDictionary].[Description] = inserted.LinkShortDesc
		, [Link_DataDictionary].[BusinessRule] = inserted.LinkLongDesc
		FROM inserted
		JOIN [ODE_Metrics_Stage].[stage].[Link_DataDictionary] d
		ON d.link_key = inserted.LinkKey
		WHERE inserted.LinkKey = @ColumnKey
		AND d.link_key = @ColumnKey
	ELSE 
		INSERT [ODE_Metrics_Stage].[stage].[Link_DataDictionary]
		SELECT LinkKey, LinkShortDesc, LinkLongDesc, GETDATE()
		FROM inserted WHERE LinkKey = @ColumnKey


	FETCH NEXT 
	FROM curUpdate INTO @ColumnKey
END
CLOSE curUpdate
DEALLOCATE curUpdate

END

GO

------------------------------------------------------------------
--Data dictionary for hubs, satellites and source tables description
--Use this view to show and update the data dictionary.
--Data dictionary information is not written directly to the Metrics Vault, 
--   but it will be available after the next Metrics Vault schedule run


CREATE VIEW [dbo].[vw_DD_Table]
AS

WITH hStable	AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Source_Table])
,sStable		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Source_Table] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,sDDStable		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_SourceTable_DataDictionary] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,hHub			AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Hub])
,sHub			AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Hub] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,sDDHub			AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_Hub_DataDictionary] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
,hSat			AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Satellite])
,lColumn_Source AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_Column_Source] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Column_Source] s 
					ON l.l_Column_Source_key = s.l_Column_Source_key WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0)
,hDV_Column		AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Column]) 
,lSat_Column	AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_column_Satellite_Column] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Column_Satellite_Column] s 
					ON l.l_column_Satellite_Column_key = s.l_Column_Satellite_Column_key WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0)
,lCol_Sat		AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_Satellite_column_Satellite] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Satellite_Column_Satellite] s 
					ON l.l_Satellite_column_Satellite_key = s.l_Satellite_Column_Satellite_key 
					WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0)
,lHub_Sat		AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_Hub_Satellite] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Hub_Satellite] s 
					ON l.l_Hub_Satellite_key = s.l_Hub_Satellite_key WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0)
,sSatDD			AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_Satellite_DataDictionary] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
,sSatellite		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Satellite] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
,sHubInt		AS (SELECT HubKey, SUM(TotalRowCount) AS TotalRowCount FROM [ODE_Metrics_Vault].[sat].[s_Hub_Integrity]  WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0
					GROUP BY HubKey)
,sSatInt		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_Satellite_Integrity]  WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)



SELECT DISTINCT 
  hStable.source_table_key		AS SourceTableKey
, sStable.[source_unique_name]	AS SourceTableName
, sDDStable.[ShortDescription]	AS SourceTableShortDesc
, sDDStable.[LongDescription]	AS SourceTableLongDesc
, CASE WHEN sStable.is_retired = 0 Then 'Current' ELSE 'Retired' END AS SourceTableFlag
, hHub.hub_key					AS HubKey
, sHub.[hub_name]				AS HubName
, sDDHub.[Description]			AS HubShortDesc
, sDDHub.[BusinessRule]			AS HubLongDesc
, CASE WHEN sHub.is_retired = 0 Then 'Current' WHEN sHub.is_retired is null then 'N/A' ELSE 'Retired' END AS HubCurrentFlag
, sHubInt.TotalRowCount			AS HubTotalRowCount
, hSat.satellite_key			AS SatelliteKey
, sSatellite.[satellite_name]	AS SatelliteName
, sSatDD.[Description]			AS SatelliteShortDesc
, sSatDD.[BusinessRule]			AS SatelliteLongDesc
, CASE WHEN sSatellite.is_retired = 0 Then 'Current' WHEN sSatellite.is_retired IS NULL then 'N/A' ELSE 'Retired' END AS SatelliteCurrentFlag
, sSatInt.TotalRowCount			AS SatelliteTotalRowCount
, sSatInt.CurrentRowCount		AS SatelliteCurrentRowCount
, sSatInt.VersionedRowCount		AS SatelliteVersionedRowCount
, sSatInt.TombstoneRowCount		AS SatelliteTombstoneRowCount
FROM hStable
LEFT JOIN lColumn_Source	ON lColumn_Source.h_DV_Source_Table_key = hStable.h_DV_Source_Table_key
LEFT JOIN hDV_Column		ON hDV_Column.h_DV_Column_key = lColumn_Source.h_DV_Column_key
INNER JOIN lSat_Column		ON lSat_Column.h_DV_Column_key = hDV_Column.h_DV_Column_key
INNER JOIN lCol_Sat			ON lCol_Sat.h_DV_Satellite_Column_key = lSat_Column.h_DV_Satellite_Column_key
INNER JOIN hSat				ON hSat.h_DV_Satellite_key = lCol_Sat.h_DV_Satellite_key
LEFT JOIN lHub_Sat			ON lHub_Sat.h_DV_Satellite_key = hSat.h_DV_Satellite_key
LEFT JOIN hHub				ON hHub.h_DV_Hub_key = lHub_Sat.h_DV_Hub_key
LEFT JOIN sDDStable			ON sDDStable.h_DV_Source_Table_key = hStable.h_DV_Source_Table_key
LEFT JOIN sSatDD			ON sSatDD.h_DV_Satellite_key = hSat.h_DV_Satellite_key
LEFT JOIN sDDHub			ON sDDHub.h_DV_Hub_key = hHub.h_DV_Hub_key
LEFT JOIN sStable			ON sStable.h_DV_Source_Table_key = hStable.h_DV_Source_Table_key
LEFT JOIN sSatellite		ON sSatellite.h_DV_Satellite_key = hSat.h_DV_Satellite_key
LEFT JOIN sHub				ON sHub.h_DV_Hub_key = hHub.h_DV_Hub_key
LEFT JOIN sHubInt			ON sHubInt.HubKey = hHub.hub_key
LEFT JOIN sSatInt			ON sSatInt.h_DV_Satellite_key = hSat.h_DV_Satellite_key

GO


Create TRIGGER [dbo].[DD_Table_InsDel_Trg] on [dbo].[vw_DD_Table]
INSTEAD OF INSERT, DELETE
AS
BEGIN
Print 'Explisit inserts and deletions on Data Dictionary tables are not allowed'
END
GO


CREATE TRIGGER [dbo].[DD_Table_Upd_Trg] ON [dbo].[vw_DD_Table]
INSTEAD OF UPDATE
AS
BEGIN

DECLARE @SourceUpdateType char(1) = 'I'
DECLARE @HubUpdateType char(1) = 'I'
DECLARE @SatUpdateType char(1) = 'I'
DECLARE @SourceKey int, @HubKey int, @SatKey int

DECLARE curUpdate CURSOR FOR
	SELECT SourceTableKey, HubKey,SatelliteKey 
	FROM inserted
OPEN curUpdate
FETCH NEXT
FROM curUpdate
INTO @SourceKey, @HubKey, @SatKey

WHILE @@FETCH_STATUS = 0
BEGIN

	SELECT @SourceUpdateType = CASE WHEN COUNT(*) > 0 THEN 'U' ELSE 'I' END 
	FROM [ODE_Metrics_Stage].[stage].[SourceTable_DataDictionary] src
	JOIN INSERTED
	ON src.source_table_key = @SourceKey

	SELECT @HubUpdateType = CASE WHEN COUNT(*) > 0 THEN 'U' ELSE 'I' END 
	FROM [ODE_Metrics_Stage].[stage].[Hub_DataDictionary] h
	JOIN INSERTED
	ON h.hub_key = @HubKey

	SELECT @SatUpdateType = CASE WHEN COUNT(*) > 0 THEN 'U' ELSE 'I' END 
	FROM [ODE_Metrics_Stage].[stage].[Satellite_DataDictionary] s
	JOIN INSERTED
	ON s.satellite_key = @SatKey

	IF @SourceUpdateType = 'U'
		UPDATE [ODE_Metrics_Stage].[stage].[SourceTable_DataDictionary]
		SET [SourceTable_DataDictionary].[ShortDescription] = inserted.SourceTableShortDesc
		, [SourceTable_DataDictionary].[LongDescription] = inserted.SourceTableLongDesc
		FROM inserted
		JOIN [ODE_Metrics_Stage].[stage].[SourceTable_DataDictionary] d
		ON d.[source_table_key] = inserted.SourceTableKey
		WHERE inserted.SourceTableKey = @SourceKey
		AND d.[source_table_key] = @SourceKey
	ELSE 
		INSERT [ODE_Metrics_Stage].[stage].[SourceTable_DataDictionary]
		SELECT SourceTableKey, SourceTableShortDesc, SourceTableLongDesc, GETDATE()
		FROM inserted WHERE SourceTableKey = @SourceKey

	IF @HubUpdateType = 'U'
		UPDATE [ODE_Metrics_Stage].[stage].[Hub_DataDictionary]
		SET [Hub_DataDictionary].[Description] = inserted.HubShortDesc
		, [Hub_DataDictionary].[BusinessRule] = inserted.HubLongDesc
		FROM inserted
		JOIN [ODE_Metrics_Stage].[stage].[Hub_DataDictionary] d
		ON d.hub_key = inserted.HubKey
		WHERE inserted.HubKey = @HubKey
		AND d.hub_key = @HubKey
	ELSE 
		INSERT [ODE_Metrics_Stage].[stage].[Hub_DataDictionary]
		SELECT HubKey, HubShortDesc, HubLongDesc, GETDATE()
		FROM inserted WHERE HubKey = @HubKey

	IF @SatUpdateType = 'U'
		UPDATE [ODE_Metrics_Stage].[stage].[Satellite_DataDictionary]
		SET [Satellite_DataDictionary].[Description] = inserted.SatelliteShortDesc
		, [Satellite_DataDictionary].[BusinessRule] = inserted.SatelliteLongDesc
		FROM inserted
		JOIN [ODE_Metrics_Stage].[stage].[Satellite_DataDictionary] d
		ON d.satellite_key = inserted.SatelliteKey
		WHERE inserted.SatelliteKey = @SatKey
		AND d.satellite_key = @SatKey
	ELSE
		INSERT [ODE_Metrics_Stage].[stage].[Satellite_DataDictionary]
		SELECT SatelliteKey, SatelliteShortDesc, SatelliteLongDesc, GETDATE()
		FROM inserted WHERE SatelliteKey = @SatKey

	FETCH NEXT FROM curUpdate 
	INTO @SourceKey, @HubKey, @SatKey
END
CLOSE curUpdate
DEALLOCATE curUpdate

END
GO

------------------------------------------------------------------
--This view shows the row number increase after the Data Vault schedule execution
--Use this view to monitor the execution


CREATE VIEW [dbo].[vw_Hub_Row_Increase]
AS
SELECT curnt.RunDate
, DATENAME(dw,curnt.RunDate) AS DayOfWeek
, curnt.[HubName]
, curnt.[SourceTableName]
, curnt.[TotalRowCount]
, curnt.[TotalRowCount] - prev.[TotalRowCount] AS RowsAddedSinceLastRun
FROM [ODE_Metrics_Vault].[sat].[s_Hub_Integrity] curnt
LEFT JOIN [ODE_Metrics_Vault].[sat].[s_Hub_Integrity] prev
ON curnt.HubKey = prev.HubKey
AND curnt.SourceTableKey = prev.SourceTableKey
AND curnt.RunDate > prev.RunDate
AND NOT EXISTS (SELECT HubKey FROM [ODE_Metrics_Vault].[sat].[s_Hub_Integrity] subQuer
				WHERE subQuer.HubKey = curnt.HubKey 
					AND subQuer.SourceTableKey = curnt.SourceTableKey 
					AND curnt.RunDate > subQuer.RunDate
					AND prev.RunDate < subQuer.RunDate)

GO

------------------------------------------------------------------
--This view shows exeptions happened during the Data Vault scheduled execution
--Use this view to monitor the execution

CREATE view [dbo].[vw_Runtime_Exception]
as
WITH hRun	AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Schedule_Run])
, sRun		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Schedule_Run] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
, hExc		AS (SELECT * FROM [ODE_Metrics_Vault].[RawHub].[h_DV_Exception])
, sExc		AS (SELECT * FROM [ODE_Metrics_Vault].[RawSat].[s_DV_Exception] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
, sSever	AS (SELECT * FROM [ODE_Metrics_Vault].[RawSat].[s_log4_Severity] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)

SELECT sRun.run_status
, cast(sRun.run_start_datetime AS datetime) AS ScheduleRunStart
, cast(sRun.run_end_datetime AS datetime) AS ScheduleRunEnd
, sExc.SystemDate AS ErrorDate
, ErrorContext
, ErrorProcedure
, ErrorLine
, ErrorMessage
, DatabaseName
, sSever.[SeverityName] AS ErrorSeverity
FROM hRun
JOIN sRun ON hRun.h_DV_Schedule_Run_key = sRun.h_DV_Schedule_Run_key
JOIN sExc ON sExc.SystemDate >= cast(sRun.[run_start_datetime] AS datetime)
  AND cast(sRun.[run_end_datetime] AS datetime) > = sExc.SystemDate
LEFT JOIN sSever ON sExc.ErrorSeverity = sSever.[SeverityId]

GO

------------------------------------------------------------------
--This view shows the row number increase after the Data Vault schedule execution
--Use this view to monitor the execution

  CREATE VIEW [dbo].[vw_Satellite_Row_Increase]
  AS
   WITH prev as ( select [s_Satellite_Integrity_key]
   ,  PreviousTotalRowCount = LAG([TotalRowCount]) over (partition by [SatelliteKey] order by [dv_rowstartdate])
   ,  PreviousCurrentRowCount = LAG(CurrentRowCount) OVER (Partition by [SatelliteKey] order by [dv_rowstartdate])
    FROM [ODE_Metrics_Vault].[sat].[s_Satellite_Integrity]
	)
  SELECT curnt.RunDate
  , DATENAME(dw,curnt.RunDate) AS DayOfWeek
  , curnt.[SatelliteName]
  , curnt.CurrentRowCount, curnt.[TotalRowCount]
  , curnt.TotalRowCount - prev.PreviousTotalRowCount AS TotalRowsAddedSinceLastRun
  , curnt.CurrentRowCount - prev.PreviousCurrentRowCount AS CurrentRowsAddedSinceLastRun
  FROM [ODE_Metrics_Vault].[sat].[s_Satellite_Integrity] curnt
  LEFT JOIN prev ON curnt.s_Satellite_Integrity_key = prev.s_Satellite_Integrity_key

GO

------------------------------------------------------------------
--This view shows satellite columns stats
--Use this view for data profiling or data reconciliation

CREATE VIEW [dbo].[vw_Satellite_Stats]
AS

WITH hSat		AS (SELECT *	FROM [ODE_Metrics_Vault].[hub].[h_DV_Satellite])
,hColumn		AS (SELECT *	FROM [ODE_Metrics_Vault].[Hub].[h_DV_Satellite_Column])
,sSat			AS (SELECT *	FROM [ODE_Metrics_Vault].[sat].[s_DV_Satellite] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
,sColumn_Integrity AS (SELECT *	FROM [ODE_Metrics_Vault].[Sat].[s_Satellite_Column_Integrity] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
, lSat_Column	AS (SELECT l.*	FROM [ODE_Metrics_Vault].[Lnk].[l_Satellite_Column_Satellite] l
					JOIN [ODE_Metrics_Vault].[Sat].[s_Link_Satellite_Column_Satellite] s 
					ON l.l_Satellite_column_Satellite_key = s.l_Satellite_Column_Satellite_key WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0)
, sSatIntegrity AS (SELECT *	FROM [ODE_Metrics_Vault].[Sat].[s_Satellite_Integrity] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)
, sColumn		AS (SELECT *	FROM [ODE_Metrics_Vault].[sat].[s_DV_Satellite_Column] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0)


SELECT 
sSat.[satellite_name] AS SatelliteName
, sSatIntegrity.[RunDate] AS DateTimeStamp
, sSatIntegrity.[CurrentRowCount]
, sColumn.[column_name] AS ColumnName
, sColumn.column_type As ColumnType
, sColumn_Integrity.[MaxLength]
, sColumn_Integrity.[MinLength]
, sColumn_Integrity.[BlankCount]
, sColumn_Integrity.[NullCount]
, sColumn_Integrity.[DomainCount]
, sColumn_Integrity.[MaxValue]
, sColumn_Integrity.[MinValue]

FROM hSat
LEFT JOIN lSat_Column		ON lSat_Column.h_DV_Satellite_key = hSat.h_DV_Satellite_key
LEFT JOIN hColumn			ON hColumn.h_DV_Satellite_Column_key = lSat_Column.h_DV_Satellite_Column_key
LEFT JOIN sColumn_Integrity	ON sColumn_Integrity.[h_DV_Satellite_Column_key] = hColumn.h_DV_Satellite_Column_key
LEFT JOIN sSatIntegrity		ON sSatIntegrity.h_DV_Satellite_key = hSat.h_DV_Satellite_key
LEFT JOIN sSat				ON sSat.h_DV_Satellite_key = hSat.h_DV_Satellite_key
LEFT JOIN sColumn			ON hColumn.h_DV_Satellite_Column_key = sColumn.h_DV_Satellite_Column_key

GO



CREATE VIEW [dbo].[vw_Schedule_Run_Stats]
/*
Schedule run statistics: state, start, finish, duration
*/
AS 
WITH hRun	AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Schedule_Run])
, sRun		AS (SELECT s.*,cast(cast(run_end_datetime as datetime)-cast(run_start_datetime as datetime) as time) as duration
 FROM [ODE_Metrics_Vault].[sat].[s_DV_Schedule_Run] s WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
, hExc		AS (SELECT * FROM [ODE_Metrics_Vault].[RawHub].[h_DV_Exception])
, sExc		AS (SELECT * FROM [ODE_Metrics_Vault].[RawSat].[s_DV_Exception] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
SELECT sRun.run_status
, cast(sRun.run_start_datetime AS datetime) AS ScheduleRunStart
, cast(sRun.run_end_datetime AS datetime) AS ScheduleRunEnd
, sRun.run_schedule_name
, sRun.run_key
, sRun.duration
, cast(sExc.SystemDate as date) AS ErrorDate
, sum(case when errorprocedure like 'dv_load%' then 1 else 0 end) as NumLoadsFailed
FROM hRun
JOIN sRun ON hRun.h_DV_Schedule_Run_key = sRun.h_DV_Schedule_Run_key
JOIN sExc ON sExc.SystemDate >= cast(sRun.[run_start_datetime] AS datetime)
  AND cast(sRun.[run_end_datetime] AS datetime) > = sExc.SystemDate
WHERE errorprocedure not in ('dv_process_queued_Agent001')
GROUP BY sRun.run_status
, cast(sRun.run_start_datetime AS datetime) 
, cast(sRun.run_end_datetime AS datetime)
, sRun.run_schedule_name
, sRun.run_key
, sRun.duration
, cast(sExc.SystemDate as date) 
GO




CREATE VIEW  [dbo].[vw_Schedule_Tasks_Run_Stats] as
/*
Schedule tasks run statistics
All durations sorted in descending order - rnum
*/
with 
  hStable	AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Source_Table])
 ,sStable	AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Source_Table] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
 ,hRunManifest as (select * from  [ODE_Metrics_Vault].[hub].[h_DV_Run_Manifest])
 ,sRunManifest as (select * from  [ODE_Metrics_Vault].[sat].[s_DV_Run_Manifest])
 ,lManifestSource as (select l.* from [ODE_Metrics_Vault].[lnk].[l_Manifest_Source] l join 
  [ODE_Metrics_Vault].[sat].[s_Link_Manifest_Source] s on l.[l_Manifest_Source_key]=s.l_Manifest_Source_key
  WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
, lRunManifest as (select  l.* from [ODE_Metrics_Vault].[lnk].[l_Run_Manifest] l join [ODE_Metrics_Vault].[sat].[s_Link_Run_Manifest] s 
on s.[l_Run_Manifest_key]=l.[l_Run_Manifest_key] )
, sScheduleRun as (select * from [ODE_Metrics_Vault].[sat].[s_DV_Schedule_Run])
, ManifestRun as 
(
select sRunManifest.run_key, sRunManifest.run_status
 ,sScheduleRun.run_schedule_name
 ,cast(sRunManifest.start_datetime as datetime) as start_datetime
 ,cast(sRunManifest.completed_datetime as datetime) as end_datetime
 , case when sRunManifest.completed_datetime is null then NULL
        else cast(cast(sRunManifest.completed_datetime as datetime)-cast(sRunManifest.start_datetime as datetime) as time) end as task_duration
 ,sStable.source_unique_name,sStable.source_table_key
 from
hRunManifest  join sRunManifest on hRunManifest.[h_DV_Run_Manifest_key]=sRunManifest.[h_DV_Run_Manifest_key]
join lManifestSource on lManifestSource.[h_DV_Run_Manifest_key]=hRunManifest.[h_DV_Run_Manifest_key]
join hStable on hStable.[h_DV_Source_Table_key]=lManifestSource.[h_DV_Source_Table_key]
join sStable on sStable.[h_DV_Source_Table_key]=hStable.[h_DV_Source_Table_key]
join lRunManifest on lRunManifest.[h_DV_Run_Manifest_key]=hRunManifest.[h_DV_Run_Manifest_key]
join sScheduleRun on  sScheduleRun.h_DV_Schedule_Run_key=lRunManifest.h_DV_Schedule_Run_key
)
select r.*, Row_number() 
                 OVER( 
                   partition BY run_key 
                   ORDER BY task_duration DESC) rnum  from ManifestRun r
GO




CREATE VIEW [dbo].[vw_Hub_Sat_Stats]
AS
/*
View combines hub and satellite loading counts statistics and provides minimal lineage information
*/
WITH 
-- source tables 
hStable	AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Source_Table] where source_table_key>=0)
,sStable		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Source_Table] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
-- hubs 
,hHub			AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Hub])
,sHub			AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Hub] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0 and hub_key>=0)
,hDV_Column		AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Column] where column_key>=0) 
-- identify source table columns - satellite columns relationship
,lColumn_Source AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_Column_Source] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Column_Source] s 
					ON l.l_Column_Source_key = s.l_Column_Source_key WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0
					and source_table_key>=0 )
,lSat_Column	AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_column_Satellite_Column] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Column_Satellite_Column] s 
					ON l.l_column_Satellite_Column_key = s.l_Column_Satellite_Column_key WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0
					and s.satellite_col_key>=0)
,lCol_Sat		AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_Satellite_column_Satellite] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Satellite_Column_Satellite] s 
					ON l.l_Satellite_column_Satellite_key = s.l_Satellite_Column_Satellite_key 
					WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0 and s.satellite_key>=0)
-- satellite 
,hSat			AS (SELECT * FROM [ODE_Metrics_Vault].[hub].[h_DV_Satellite] where satellite_key>=0 )
,sSatellite		AS (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Satellite] WHERE dv_row_is_current = 1 AND dv_is_tombstone = 0 and satellite_key>=0)
-- hub - satellite relationship
,lHub_Sat		AS (SELECT l.* FROM [ODE_Metrics_Vault].[lnk].[l_Hub_Satellite] l
					JOIN [ODE_Metrics_Vault].[sat].[s_Link_Hub_Satellite] s 
					ON l.l_Hub_Satellite_key = s.l_Hub_Satellite_key WHERE s.dv_row_is_current = 1 AND s.dv_is_tombstone = 0)
,sHubSat as (select distinct hHub.hub_key,sSatellite.satellite_key, sHub.hub_name,sSatellite.satellite_name 
					  from hHub join sHub on hHub.h_DV_Hub_key=sHub.h_DV_Hub_key 
                           left join lHub_Sat on lHub_Sat.h_DV_Hub_key=hHub.h_DV_Hub_key
						   left join sSatellite on sSatellite.h_DV_Satellite_key=lHub_Sat.h_DV_Satellite_key)
-- source version
,sSource_Ver as (SELECT * FROM [ODE_Metrics_Vault].[sat].[s_DV_Source_Version] WHERE [dv_row_is_current] = 1 AND [dv_is_tombstone] = 0)
-- source table - satellite - hub relationships 
,sl_SatSource as (select distinct lColumn_Source.[h_DV_Source_Table_key], lCol_Sat.[h_DV_Satellite_key] 
from lColumn_Source join lSat_Column on lColumn_Source.[h_DV_Column_key]=lSat_Column.[h_DV_Column_key]
join lCol_Sat on lCol_Sat.[h_DV_Satellite_Column_key]=lSat_Column.[h_DV_Satellite_Column_key])
,sSat_Source as (select sl_SatSource.*, sStable.source_unique_name as source_table_name,sStable.source_table_key
, ssatellite.satellite_key, ssatellite.satellite_name
 from hStable join sSTable on hStable.h_DV_Source_Table_key=sStable.h_DV_Source_Table_key
join sl_SatSource on sl_SatSource.h_DV_Source_Table_key=hStable.h_DV_Source_Table_key
join ssatellite on ssatellite.h_DV_Satellite_key=sl_SatSource.h_DV_Satellite_key
left join sHubSat on ssatellite.hub_key=sHubSat.hub_key)
-- hub row counts stats
,sHubSt   as 
(SELECT cast(curnt.RunDate as date) as RunDate
, curnt.[HubName]
, curnt.HubKey
, curnt.[SourceTableName]
, sSource_Ver.Source_Table_Key as SourceTableKey
,sSat_Source.satellite_key
, sSat_Source.satellite_name
, curnt.[TotalRowCount]
, curnt.[TotalRowCount] - prev.[TotalRowCount] AS RowsAddedSinceLastRun
FROM [ODE_Metrics_Vault].[sat].[s_Hub_Integrity] curnt
LEFT JOIN [ODE_Metrics_Vault].[sat].[s_Hub_Integrity] prev
ON curnt.HubKey = prev.HubKey
AND curnt.SourceVersionKey = prev.SourceVersionKey
AND curnt.RunDate > prev.RunDate
AND NOT EXISTS (SELECT HubKey FROM [ODE_Metrics_Vault].[sat].[s_Hub_Integrity] subQuer
				WHERE subQuer.HubKey = curnt.HubKey 
					AND subQuer.SourceVersionKey = curnt.SourceVersionKey
					AND curnt.RunDate > subQuer.RunDate
					AND prev.RunDate < subQuer.RunDate)
left join sSource_Ver on sSource_Ver.source_version_key=curnt.SourceVersionKey
left join sSat_Source on sSat_Source.Source_Table_key=sSource_Ver.source_table_key)
-- satellite row counts stats
, sSatSt as (SELECT cast(curnt.RunDate as date) as RunDate
  , sSatellite.hub_key
  , sHubSat.hub_name
  , curnt.SatelliteKey
  , curnt.[SatelliteName]
  ,sSat_Source.source_table_key
  ,sSat_Source.source_table_name
  , curnt.CurrentRowCount
  , curnt.[TotalRowCount]
  , curnt.[TombstoneRowCount]
  , curnt.TotalRowCount - prev.PreviousTotalRowCount AS TotalRowsAddedSinceLastRun
  , curnt.CurrentRowCount - prev.PreviousCurrentRowCount AS CurrentRowsAddedSinceLastRun
  FROM [ODE_Metrics_Vault].[sat].[s_Satellite_Integrity] curnt
  LEFT JOIN (select [s_Satellite_Integrity_key]
   ,  PreviousTotalRowCount = LAG([TotalRowCount]) over (partition by [SatelliteKey] order by [dv_rowstartdate])
   ,  PreviousCurrentRowCount = LAG(CurrentRowCount) OVER (Partition by [SatelliteKey] order by [dv_rowstartdate])
    FROM [ODE_Metrics_Vault].[sat].[s_Satellite_Integrity]) prev ON curnt.s_Satellite_Integrity_key = prev.s_Satellite_Integrity_key
	left join sSat_Source on sSat_Source.h_DV_Satellite_key=curnt.h_DV_Satellite_key
	left join sSatellite on sSatellite.h_DV_Satellite_key=curnt.h_DV_Satellite_key
	left join sHubSat on sHubSat.hub_key=sSatellite.hub_key and sHubSat.satellite_key=sSatellite.satellite_key
)
SELECT DISTINCT isnull(h.RunDate, s.RunDate) as rundate
, isnull(h.SourceTableKey,s.source_table_key)	AS SourceTableKey
, isnull(h.[SourceTableName],s.source_table_name)	AS SourceTableName
, isnull(h.HubKey,s.hub_key)				AS HubKey
, isnull(h.HubName, s.hub_name)				AS HubName
, h.TotalRowCount			AS HubTotalRowCount
, h.RowsAddedSinceLastRun  as HubRowsAddedSinceLastRun
, isnull(h.satellite_key ,s.SatelliteKey)	AS SatelliteKey
, isnull(h.satellite_name,s.SatelliteName)	AS SatelliteName
, s.TotalRowCount			AS SatelliteTotalRowCount
, s.CurrentRowCount		AS SatelliteCurrentRowCount
, s.CurrentRowsAddedSinceLastRun as SatCurrentRowsAddedSinceLastRun
, s.TotalRowsAddedSinceLastRun as SatelliteRowsAddedSinceLastRun
, s.TombstoneRowCount		AS SatelliteTombstoneRowCount
FROM sHubSt h full join sSatSt s on h.HubKey=s.hub_key and h.RunDate=s.RunDate and h.satellite_key=s.SatelliteKey
GO

