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

ALTER PROC [purview].[usp_Load_Purview_Files] AS
/******************************************************************************
*  Stored Procedure Name: [purview].[usp_Load_Purview_Files] 
*  Input Parameters: 
*  Example: EXEC [purview].[usp_Load_Purview_Files]  
*
*  History:
*  Date:				Action:								Developer: 
*  2025-04-08			Initial version						Darpan Desai & Jemini Joseph
******************************************************************************/
Begin
	DECLARE @Cntr int=1,
		@MaxRows int,
		@FileName varchar(200),
		@BatchID varchar(100),
		@FileID bigint,
		@StartTime datetime,
		@FileCount int

	if object_id('tempdb..#FileList') is not null
		drop table #FileList
	CREATE TABLE #FileList (FileID int,BatchID varchar(100), FileName varchar(200))

	INSERT INTO #FileList select ROW_NUMBER() OVER (ORDER BY FileName) as FileID,BatchID, FileName from stage.PurviewJSONFileList

	select @MaxRows=max(FileID) from #FileList
	while (@Cntr <=@MaxRows)
		Begin
			select @FileName=FileName,@BatchID=BatchID from #FileList where FileID=@Cntr
			select @FileCount=count(*) from [etl].[ImportedFilesPurview]
			where filename=@FileName and ImportStatus is not null

			set @StartTime=convert(varchar(25),GETDATE(),121)
			IF isnull(@FileCount,0)=0
				BEGIN
					select @FileID=max(FileID) from [etl].[ImportedFilesPurview]
					IF isnull(@FileID,0)=0
						set @FileID=1
					ELSE
						set @FileID=@FileID+1
					INSERT INTO [etl].[ImportedFilesPurview](
						[FileID]
						,[BatchID]
						,[StartTime]
						,[FileName]
						)
					VALUES (@FileID
						,@BatchID
						,@StartTime
						,@FileName
					)
					EXEC  purview.[usp_Insert_Purview_WorkloadActivities] @FileID,@FileName
				END
			set @Cntr=@Cntr+1
		End
		select distinct FileName from [etl].[ImportedFilesPurview]
		where batchid=@BatchID
	END
