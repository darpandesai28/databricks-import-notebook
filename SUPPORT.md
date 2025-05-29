# TODO: The maintainer of this repo has not yet edited this file

**REPO OWNER**: Do you want Customer Service & Support (CSS) support for this product/project?

- **No CSS support:** Fill out this template with information about how to file issues and get help.
- **Yes CSS support:** Fill out an intake form at [aka.ms/spot](https://aka.ms/spot). CSS will work with/help you to determine next steps. More details also available at [aka.ms/onboardsupport](https://aka.ms/onboardsupport).
- **Not sure?** Fill out a SPOT intake as though the answer were "Yes". CSS will help you decide.

*Then remove this first heading from this SUPPORT.MD file before publishing your repo.*

# Support

## How to file issues and get help  

This project uses GitHub Issues to track bugs and feature requests. Please search the existing 
issues before filing new issues to avoid duplicates.  For new issues, file your bug or 
feature request as a new Issue.

For help and questions about using this project, please **REPO MAINTAINER: INSERT INSTRUCTIONS HERE 
FOR HOW TO ENGAGE REPO OWNERS OR COMMUNITY FOR HELP. COULD BE A STACK OVERFLOW TAG OR OTHER
CHANNEL. WHERE WILL YOU HELP PEOPLE?**.

## Microsoft Support Policy  

Support for this **PROJECT or PRODUCT** is limited to the resources listed above.

/****** Object:  StoredProcedure [purview].[usp_Insert_Purview_WorkloadActivities]    Script Date: 5/29/2025 3:22:46 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROC [purview].[usp_Insert_Purview_WorkloadActivities] @FileID [NVARCHAR](250),@FileName [NVARCHAR](250) AS
/******************************************************************************
*  Stored Procedure Name: [purview].[usp_Insert_Purview_WorkloadActivities]
*  Input Parameters:  @FileID ,@FileName
*  Example: EXEC purview.[usp_Insert_Purview_WorkloadActivities] @FileID=1,  @FileName='Purview_ActivityExplorer_PowerBI2025421531_0.json'
*
*  History:
*  Date:				Action:								Developer: 
*  2025-03-28			Initial version						Darpan Desai & Jemini 
******************************************************************************/
begin
/*--------------------------------- Insert JSON file contents into stage.ImpPurviewJSONData table ---------------------------------*/
Declare @SQL NVARCHAR(max),
		@Flag VARCHAR(250),
		@JSONUrl VARCHAR(250),
		@TotalCount BIGINT,
		@StorageAccount VARCHAR(250),
		@Container VARCHAR(250),
		@RowsProcessed int,
		@WorkLoad varchar(50),
		@EndTime DateTime

SELECT @StorageAccount= ImportStorageAccount,@Container = ImportStorageContainer FROM [etl].[StorageConfigs] where Process = 'PURVIEW'



SET @JSONUrl = 'https://' + @StorageAccount + '.blob.core.usgovcloudapi.net/' + @Container + '/' + @FileName

begin try

	TRUNCATE TABLE stage.ImpPurviewJSONData

	SET @SQL='COPY INTO stage.ImpPurviewJSONData(JSONData)
		FROM '''+@JSONUrl+'''
		WITH 
		(
		FILE_TYPE = ''CSV''
		,fieldterminator =''0x0b''
		,fieldquote = ''0x0b''
		,rowterminator = ''0x0c''  /* Override This if a Json document and not single object */
		,CREDENTIAL=(IDENTITY= ''Managed Identity'')
		)
		'
	EXEC sp_executesql @SQL

	if object_id('tempdb.dbo.#temp') is not null
		drop table #temp

		select @Flag=left(trim(JSONData),1) from stage.ImpPurviewJSONData
		
		if @Flag ='['
			begin
				SELECT b.[key] as Id,
				   c.[key] as [ColumnName],CAST(c.[value] as varchar(8000)) as [ColumnValue],@FileID as FileID
				into #temp
				FROM stage.ImpPurviewJSONData a
				CROSS APPLY OPENJSON(a.JSONData) b
				CROSS APPLY OPENJSON(b.[value]) c
			end
			else
				begin
					SELECT a.[Id] as Id,
					b.[key] as [ColumnName],CAST(b.[value] as varchar(8000)) as [ColumnValue],@FileID as FileID
					into #temp
					--select * 
					FROM stage.ImpPurviewJSONData a
					CROSS APPLY OPENJSON(a.JSONData) b
				end

	
	DECLARE @columns AS NVARCHAR(MAX), 
			@sqlStatement AS NVARCHAR(MAX),
			@tableName AS NVARCHAR(MAX);
	-- Get the distinct column names for pivoting (ColumnName values)
	SELECT @columns = STRING_AGG(QUOTENAME(ColumnName), ', ')
	FROM (SELECT DISTINCT ColumnName FROM #temp) AS ColumnNames;
    
	-- Create the dynamic SQL query for pivoting

	SET @sqlStatement = '
	SELECT * into #Workload
	FROM (
		SELECT Id,ColumnName, ColumnValue,FileID
		FROM #temp
	) AS SourceTable
	PIVOT (
		MAX(ColumnValue) FOR ColumnName IN (' + @columns + ')
	) AS PivotTable;';
 
	-- Execute the dynamic SQL
	if object_id('tempdb.dbo.#Workload') is not null
			drop table #Workload
	EXEC sp_executesql @sqlStatement;

	Declare @InsertSQL nvarchar(max),
			@SelectSQL nvarchar(max),
			@FinalSQL nvarchar(max),
			@CountSQL nvarchar(max)

	SELECT @InsertSQL = 'insert into  [purview].[ActivityExplorer] ([FileID],'+ STRING_AGG(ColumnName, ',') +')'
	FROM 
	(select distinct '['+convert(varchar(200),ColumnName)+']' as ColumnName from #temp) a



	SELECT @SelectSQL='select [FileID],'+ STRING_AGG(ColumnName, ',') +' from #Workload'
	FROM 
	(select distinct '['+convert(varchar(200),ColumnName)+']' as ColumnName from #temp) a

	set @FinalSQL=@InsertSQL+' ' + @SelectSQL
	EXEC sp_executesql @FinalSQL;

	SELECT @RowsProcessed=COUNT(*),@WorkLoad=max(WorkLoad) from [purview].[ActivityExplorer] where FileID=@FileID
	set @EndTime=convert(varchar(25),GETDATE(),121)

	update [etl].[ImportedFilesPurview]
		set [EndTime]=@EndTime,
			[ImportStatus]=1,
			[Workload]=@WorkLoad,
			[ImportStorageAccount]=@StorageAccount,
			[ImportContainer]=@Container,
			[JSONFileNameUrl]=@JSONUrl,
			[RowsProcessed]=@RowsProcessed,
			[ErrorMessage]=null
		where FileID=@FileID 

end try
  begin catch
	    DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;
        SELECT  @ErrorSeverity = ISNULL(ERROR_SEVERITY(), 16), @ErrorState =  ERROR_STATE(), @ErrorMessage =  ERROR_MESSAGE();

		SELECT @RowsProcessed=COUNT(*),@WorkLoad=max(WorkLoad) from [purview].[ActivityExplorer] where FileID=@FileID
		set @EndTime=convert(varchar(25),GETDATE(),121)
		
		update [etl].[ImportedFilesPurview]
		set [EndTime]=@EndTime,
			[ImportStatus]=0,
			[Workload]=@WorkLoad,
			[ImportStorageAccount]=@StorageAccount,
			[ImportContainer]=@Container,
			[JSONFileNameUrl]=@JSONUrl,
			[RowsProcessed]=@RowsProcessed,
			[ErrorMessage]=@ErrorMessage
		where FileID=@FileID 

		
        RAISERROR('Error in SP [usp_Insert_Purview_WorkloadActivities]: %s', @ErrorSeverity, @ErrorState, @ErrorMessage);
	end catch

End
