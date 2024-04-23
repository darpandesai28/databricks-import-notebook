/****** Object:  User [S-EITAAS-CAS-CDE-AS-DBViewSPVW]    Script Date: 4/23/2024 10:49:58 AM ******/
CREATE USER [S-EITAAS-CAS-CDE-AS-DBViewSPVW] FROM  EXTERNAL PROVIDER 
GO
/****** Object:  User [S-EITAAS-CAS-CDE-SQL-DBReader]    Script Date: 4/23/2024 10:49:58 AM ******/
CREATE USER [S-EITAAS-CAS-CDE-SQL-DBReader] FROM  EXTERNAL PROVIDER 
GO
/****** Object:  User [S-EITAAS-CAS-CDE-SQL-DBWriter]    Script Date: 4/23/2024 10:49:58 AM ******/
CREATE USER [S-EITAAS-CAS-CDE-SQL-DBWriter] FROM  EXTERNAL PROVIDER 
GO
sys.sp_addrolemember @rolename = N'db_datareader', @membername = N'S-EITAAS-CAS-CDE-AS-DBViewSPVW'
GO
sys.sp_addrolemember @rolename = N'db_datareader', @membername = N'S-EITAAS-CAS-CDE-SQL-DBReader'
GO
/****** Object:  Schema [etlaudit]    Script Date: 4/23/2024 10:49:58 AM ******/
CREATE SCHEMA [etlaudit]
GO
/****** Object:  Table [etlaudit].[AuditProcessExecution]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etlaudit].[AuditProcessExecution](
	[ProcessExecutionKey] [int] IDENTITY(1,1) NOT NULL,
	[ParentProcessExecutionKey] [int] NULL,
	[ExtractionTimeKey] [int] NULL,
	[ProcessName] [varchar](150) NOT NULL,
	[SuccessfulProcessingIndicator] [char](1) NULL,
	[Duration]  AS (case when datediff(day,(0),[ExecutionStopDatetime]-[ExecutionStartDatetime])=(0) then case when datepart(hour,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(0) then case when datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(0) then case when datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(0) then CONVERT([varchar](3),datepart(millisecond,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' ms' else case when datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Secs ' end end else case when datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Min, ' else CONVERT([varchar](2),datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Mins, ' end+case when datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Secs ' end end else (case when datepart(hour,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(hour,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Hour, ' else CONVERT([varchar](2),datepart(hour,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Hours, ' end+case when datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Min, ' else CONVERT([varchar](2),datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Mins, ' end)+case when datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Secs ' end end else ((case when datediff(day,(0),[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](10),datediff(day,(0),[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Day, ' else CONVERT([varchar](10),datediff(day,(0),[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Days, ' end+case when datepart(hour,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(hour,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Hour, ' else CONVERT([varchar](2),datepart(hour,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Hours, ' end)+case when datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Min, ' else CONVERT([varchar](2),datepart(minute,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Mins, ' end)+case when datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExecutionStopDatetime]-[ExecutionStartDatetime]))+' Secs ' end end),
	[ExecutionStartDatetime] [datetime] NULL,
	[ExecutionStopDatetime] [datetime] NULL,
	[Audit_Created_Datetime] [datetime] NOT NULL,
	[Audit_Created_Date]  AS (CONVERT([date],[Audit_Created_Datetime])),
	[Audit_Created_User] [nvarchar](100) NOT NULL,
	[Audit_Created_Host] [nvarchar](64) NOT NULL,
	[Audit_Modified_Datetime] [datetime] NULL,
	[Audit_Modified_Date]  AS (CONVERT([date],[Audit_Modified_Datetime])),
	[Audit_Modified_User] [nvarchar](100) NULL,
	[Audit_Modified_Host] [nvarchar](64) NULL,
 CONSTRAINT [PK_AuditProcessExecution_ProcessExecutionKey] PRIMARY KEY CLUSTERED 
(
	[ProcessExecutionKey] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, FILLFACTOR = 85, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [etlaudit].[vw_AuditJobStatus]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [etlaudit].[vw_AuditJobStatus]
AS
SELECT 
	ProcessExecutionKey,
	ParentProcessExecutionKey,
	ExtractionTimeKey,
	ProcessName,
	SuccessfulProcessingIndicator,
	Duration,
    ExecutionStartDatetime,
	ExecutionStopDatetime
FROM
	etlaudit.AuditProcessExecution
GO
/****** Object:  Table [etlaudit].[AuditErrorLog]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etlaudit].[AuditErrorLog](
	[ErrorLogKey] [int] IDENTITY(1,1) NOT NULL,
	[ProcessExecutionKey] [int] NOT NULL,
	[DimOrFactName] [varchar](150) NULL,
	[UserName] [sysname] NULL,
	[ErrorDate] [datetime] NULL,
	[ErrorProcedure] [nvarchar](max) NULL,
	[ErrorLine] [int] NULL,
	[DecisionPointID] [varchar](50) NULL,
	[DecisionPoint] [varchar](255) NULL,
	[ErrorMessage] [nvarchar](max) NULL,
	[ErrorNumber] [int] NULL,
	[ErrorSeverity] [int] NULL,
	[ErrorState] [int] NULL,
	[ErrorType] [varchar](20) NULL,
	[TableColumn] [varchar](80) NULL,
	[SettledFlag] [varchar](11) NULL,
	[SettledExplanation] [varchar](250) NULL,
	[Audit_Created_Datetime] [datetime] NOT NULL,
	[Audit_Created_Date]  AS (CONVERT([date],[Audit_Created_Datetime])),
	[Audit_Created_User] [nvarchar](64) NOT NULL,
	[Audit_Created_Host] [nvarchar](64) NOT NULL,
	[Audit_Modified_Datetime] [datetime] NULL,
	[Audit_Modified_Date]  AS (CONVERT([date],[Audit_Modified_Datetime])),
	[Audit_Modified_User] [nvarchar](64) NULL,
	[Audit_Modified_Host] [nvarchar](64) NULL,
 CONSTRAINT [PK_AuditErrorLog_ErrorLogKey] PRIMARY KEY CLUSTERED 
(
	[ErrorLogKey] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [etlaudit].[vw_AuditErrors]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [etlaudit].[vw_AuditErrors]
AS
SELECT
    ErrorLogKey,
    ProcessExecutionKey,
    DimOrFactName,
    UserName,
    ErrorDate,
    ErrorProcedure,
    DecisionPointID,
    DecisionPoint,
    ErrorMessage,
    ErrorNumber
FROM
    etlaudit.AuditErrorLog 
GO
/****** Object:  Table [etlaudit].[Audit]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etlaudit].[Audit](
	[AuditKey] [int] IDENTITY(1,1) NOT NULL,
	[TableProcessKey] [int] NOT NULL,
	[DataFileKey] [int] NOT NULL,
	[BranchName] [varchar](150) NOT NULL,
	[BranchRowCnt] [int] NULL,
	[ProcessingSummaryGroup] [varchar](150) NOT NULL,
	[Audit_Created_Datetime] [datetime] NOT NULL,
	[Audit_Created_Date]  AS (CONVERT([date],[Audit_Created_Datetime])),
	[Audit_Created_User] [nvarchar](64) NOT NULL,
	[Audit_Created_Host] [nvarchar](64) NOT NULL,
	[Audit_Modified_Datetime] [datetime] NULL,
	[Audit_Modified_Date]  AS (CONVERT([date],[Audit_Modified_Datetime])),
	[Audit_Modified_User] [nvarchar](64) NULL,
	[Audit_Modified_Host] [nvarchar](64) NULL,
 CONSTRAINT [PK_AUDIT_AuditKey] PRIMARY KEY CLUSTERED 
(
	[AuditKey] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [etlaudit].[AuditTableProcessing]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etlaudit].[AuditTableProcessing](
	[TableProcessKey] [int] IDENTITY(1,1) NOT NULL,
	[ProcessExecutionKey] [int] NOT NULL,
	[SuccessfulProcessingIndicator] [char](1) NULL,
	[TableInitialRowCnt] [int] NULL,
	[TableFinalRowCnt] [int] NULL,
	[ExtractRowCnt] [int] NULL,
	[InsertRowCnt] [int] NULL,
	[UpdateRowCnt] [int] NULL,
	[DeleteRowCnt] [int] NULL,
	[ErrorRowCnt] [int] NULL,
	[SourceDatabase] [varchar](100) NULL,
	[SourceTableName] [varchar](100) NULL,
	[DestinationDatabase] [varchar](100) NULL,
	[DestinationTableName] [varchar](100) NULL,
	[Audit_Created_Datetime] [datetime] NOT NULL,
	[Audit_Created_Date]  AS (CONVERT([date],[Audit_Created_Datetime])),
	[Audit_Created_User] [nvarchar](64) NOT NULL,
	[Audit_Created_Host] [nvarchar](64) NOT NULL,
	[Audit_Modified_Datetime] [datetime] NULL,
	[Audit_Modified_Date]  AS (CONVERT([date],[Audit_Modified_Datetime])),
	[Audit_Modified_User] [nvarchar](64) NULL,
	[Audit_Modified_Host] [nvarchar](64) NULL,
 CONSTRAINT [PK_AuditTableProcessing_TableProcessKey] PRIMARY KEY CLUSTERED 
(
	[TableProcessKey] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [etlaudit].[AuditExtractionTime]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etlaudit].[AuditExtractionTime](
	[ExtractionTimeKey] [int] IDENTITY(1,1) NOT NULL,
	[ExtractionSpan]  AS (case when datediff(day,(0),[ExtractStopDatetime]-[ExtractStartDatetime])=(0) then case when datepart(hour,[ExtractStopDatetime]-[ExtractStartDatetime])=(0) then case when datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime])=(0) then case when datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime])=(0) then CONVERT([varchar](3),datepart(millisecond,[ExtractStopDatetime]-[ExtractStartDatetime]))+' ms' else case when datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Secs ' end end else case when datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Min, ' else CONVERT([varchar](2),datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Mins, ' end+case when datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Secs ' end end else (case when datepart(hour,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(hour,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Hour, ' else CONVERT([varchar](2),datepart(hour,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Hours, ' end+case when datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Min, ' else CONVERT([varchar](2),datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Mins, ' end)+case when datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Secs ' end end else ((case when datediff(day,(0),[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](10),datediff(day,(0),[ExtractStopDatetime]-[ExtractStartDatetime]))+' Day, ' else CONVERT([varchar](10),datediff(day,(0),[ExtractStopDatetime]-[ExtractStartDatetime]))+' Days, ' end+case when datepart(hour,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(hour,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Hour, ' else CONVERT([varchar](2),datepart(hour,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Hours, ' end)+case when datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Min, ' else CONVERT([varchar](2),datepart(minute,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Mins, ' end)+case when datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime])=(1) then CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Sec ' else CONVERT([varchar](2),datepart(second,[ExtractStopDatetime]-[ExtractStartDatetime]))+' Secs ' end end),
	[ExtractStartDatetime] [datetime] NULL,
	[ExtractStopDatetime] [datetime] NULL,
	[SourceSystem] [varchar](50) NULL,
	[Comment] [varchar](50) NULL,
	[LastUpdated] [datetime] NULL,
	[Audit_Created_Datetime] [datetime] NOT NULL,
	[Audit_Created_Date]  AS (CONVERT([date],[Audit_Created_Datetime])),
	[Audit_Created_User] [nvarchar](100) NOT NULL,
	[Audit_Created_Host] [nvarchar](64) NOT NULL,
	[Audit_Modified_Datetime] [datetime] NULL,
	[Audit_Modified_Date]  AS (CONVERT([date],[Audit_Modified_Datetime])),
	[Audit_Modified_User] [nvarchar](100) NULL,
	[Audit_Modified_Host] [nvarchar](64) NULL,
 CONSTRAINT [PK_AuditExtractionTime_ExtractionTimeKey] PRIMARY KEY NONCLUSTERED 
(
	[ExtractionTimeKey] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [etlaudit].[vw_AuditTableProcessing]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [etlaudit].[vw_AuditTableProcessing]
AS
SELECT
	APE.ProcessExecutionKey,
	APE.ParentProcessExecutionKey,
	AET.ExtractionTimeKey,
	AET.ExtractionSpan,
	APE.ProcessName,
	APE.Duration,
	APE.ExecutionStartDatetime,
	APE.ExecutionStopDatetime,
	APE.SuccessfulProcessingIndicator,
	ATP.TableProcessKey,
	ATP.SourceDatabase,
	ATP.SourceTableName,
	ATP.DestinationDatabase,
	ATP.DestinationTableName,
	ATP.ExtractRowCnt,
	ATP.InsertRowCnt,
	ATP.UpdateRowCnt,
	ATP.DeleteRowCnt,
	ATP.ErrorRowCnt,
	ATP.TableInitialRowCnt,
	ATP.TableFinalRowCnt,
	A.AuditKey,
	A.DataFileKey,
	A.BranchName,
	A.BranchRowCnt,
	A.ProcessingSummaryGroup
FROM
	(
		SELECT
			ProcessExecutionKey,
			ParentProcessExecutionKey,
			ExtractionTimeKey,
			ProcessName,
			SuccessfulProcessingIndicator,
			Duration,
			ExecutionStartDatetime,
			ExecutionStopDatetime
		FROM
			etlaudit.AuditProcessExecution
	
	) APE
	LEFT OUTER JOIN
	etlaudit.AuditTableProcessing ATP ON
	APE.ProcessExecutionKey = ATP.ProcessExecutionKey
	LEFT OUTER JOIN
	etlaudit.Audit A ON
	ATP.TableProcessKey = A.TableProcessKey
	LEFT OUTER JOIN
	etlaudit.AuditExtractionTime AET ON
	APE.ExtractionTimeKey = AET.ExtractionTimeKey
GO
/****** Object:  Table [etlaudit].[ConstantsMeta]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [etlaudit].[ConstantsMeta](
	[ConstantName] [varchar](100) NOT NULL,
	[ConstantValue] [varchar](100) NOT NULL,
	[Description] [varchar](1000) NULL,
	[DeprecatedFlag] [varchar](14) NULL,
	[Audit_Created_Datetime] [datetime] NOT NULL,
	[Audit_Created_Date]  AS (CONVERT([date],[Audit_Created_Datetime])),
	[Audit_Created_User] [nvarchar](64) NOT NULL,
	[Audit_Created_Host] [nvarchar](64) NOT NULL,
	[Audit_Modified_Datetime] [datetime] NULL,
	[Audit_Modified_Date]  AS (CONVERT([date],[Audit_Modified_Datetime])),
	[Audit_Modified_User] [nvarchar](64) NULL,
	[Audit_Modified_Host] [nvarchar](64) NULL,
 CONSTRAINT [PK_ConstantsMeta_ConstantName] PRIMARY KEY CLUSTERED 
(
	[ConstantName] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, FILLFACTOR = 80, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [etlaudit].[Audit] ADD  CONSTRAINT [DF_Audit_Audit_Created_Datetime]  DEFAULT (getdate()) FOR [Audit_Created_Datetime]
GO
ALTER TABLE [etlaudit].[Audit] ADD  CONSTRAINT [DF_Audit_Audit_Created_User]  DEFAULT (suser_name()) FOR [Audit_Created_User]
GO
ALTER TABLE [etlaudit].[Audit] ADD  CONSTRAINT [DF_Audit_Audit_Created_Host]  DEFAULT (host_name()) FOR [Audit_Created_Host]
GO
ALTER TABLE [etlaudit].[AuditErrorLog] ADD  CONSTRAINT [DF_AuditErrorLog_Audit_Created_Datetime]  DEFAULT (getdate()) FOR [Audit_Created_Datetime]
GO
ALTER TABLE [etlaudit].[AuditErrorLog] ADD  CONSTRAINT [DF_AuditErrorLog_Audit_Created_User]  DEFAULT (suser_name()) FOR [Audit_Created_User]
GO
ALTER TABLE [etlaudit].[AuditErrorLog] ADD  CONSTRAINT [DF_AuditErrorLog_Audit_Created_Host]  DEFAULT (host_name()) FOR [Audit_Created_Host]
GO
ALTER TABLE [etlaudit].[AuditExtractionTime] ADD  CONSTRAINT [DF_AuditExtractionTime_LastUpdated]  DEFAULT (getdate()) FOR [LastUpdated]
GO
ALTER TABLE [etlaudit].[AuditExtractionTime] ADD  CONSTRAINT [DF_AuditExtractionTime_Audit_Created_Datetime]  DEFAULT (getdate()) FOR [Audit_Created_Datetime]
GO
ALTER TABLE [etlaudit].[AuditExtractionTime] ADD  CONSTRAINT [DF_AuditExtractionTime_Audit_Created_User]  DEFAULT (suser_name()) FOR [Audit_Created_User]
GO
ALTER TABLE [etlaudit].[AuditExtractionTime] ADD  CONSTRAINT [DF_AuditExtractionTime_Audit_Created_Host]  DEFAULT (host_name()) FOR [Audit_Created_Host]
GO
ALTER TABLE [etlaudit].[AuditProcessExecution] ADD  CONSTRAINT [DF_AuditProcessExecution_Audit_Created_Datetime]  DEFAULT (getdate()) FOR [Audit_Created_Datetime]
GO
ALTER TABLE [etlaudit].[AuditProcessExecution] ADD  CONSTRAINT [DF_AuditProcessExecution_Audit_Created_User]  DEFAULT (suser_name()) FOR [Audit_Created_User]
GO
ALTER TABLE [etlaudit].[AuditProcessExecution] ADD  CONSTRAINT [DF_AuditProcessExecution_Audit_Created_Host]  DEFAULT (host_name()) FOR [Audit_Created_Host]
GO
ALTER TABLE [etlaudit].[AuditTableProcessing] ADD  CONSTRAINT [DF_AuditTableProcessing_Audit_Created_Datetime]  DEFAULT (getdate()) FOR [Audit_Created_Datetime]
GO
ALTER TABLE [etlaudit].[AuditTableProcessing] ADD  CONSTRAINT [DF_AuditTableProcessing_Audit_Created_User]  DEFAULT (suser_name()) FOR [Audit_Created_User]
GO
ALTER TABLE [etlaudit].[AuditTableProcessing] ADD  CONSTRAINT [DF_AuditTableProcessing_Audit_Created_Host]  DEFAULT (host_name()) FOR [Audit_Created_Host]
GO
ALTER TABLE [etlaudit].[ConstantsMeta] ADD  CONSTRAINT [DF_ConstantsMeta_Audit_Created_Datetime]  DEFAULT (getdate()) FOR [Audit_Created_Datetime]
GO
ALTER TABLE [etlaudit].[ConstantsMeta] ADD  CONSTRAINT [DF_ConstantsMeta_Audit_Created_User]  DEFAULT (suser_name()) FOR [Audit_Created_User]
GO
ALTER TABLE [etlaudit].[ConstantsMeta] ADD  CONSTRAINT [DF_ConstantsMeta_Audit_Created_Host]  DEFAULT (host_name()) FOR [Audit_Created_Host]
GO
/****** Object:  StoredProcedure [etlaudit].[resetDB]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [etlaudit].[resetDB] AS

begin
	truncate table [etlaudit].[AuditExtractionTime];
	truncate table [etlaudit].[AuditProcessExecution];

end;
GO
/****** Object:  StoredProcedure [etlaudit].[usp_AdminAddAuditingColumns]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_AdminAddAuditingColumns]
					@DatabaseName VARCHAR(128),
					@SchemaName	VARCHAR(128),
					@TableName VARCHAR(128),
					@Debug BIT = 0,
					@IndexFileGroup VARCHAR(100) = '[INDEXES]',
					@DataCompression VARCHAR(4) = NULL,
					@CreateDateTimeColumn VARCHAR(128) = 'Audit_Created_Datetime',
					@CreateDateColumn VARCHAR(128) = 'Audit_Created_Date',
					@CreateUserColumn VARCHAR(128) = 'Audit_Created_User',
					@CreateHostColumn VARCHAR(128) = 'Audit_Created_Host',
					@ModifyDateTimeColumn VARCHAR(128) = 'Audit_Modified_Datetime',
					@ModifyDateColumn VARCHAR(128) = 'Audit_Modified_Date',
					@ModifyUserColumn VARCHAR(128) = 'Audit_Modified_User',
					@ModifyHostColumn VARCHAR(128) = 'Audit_Modified_Host'
AS
BEGIN

	SET NOCOUNT ON

	/*
	-- Debugging Purposes

	DECLARE @DatabaseName VARCHAR(128)
	DECLARE @SchemaName VARCHAR(128)
	DECLARE @TableName VARCHAR(128)
	DECLARE @Debug BIT
	DECLARE @IndexFileGroup VARCHAR(30)
	DECLARE @DataCompression VARCHAR(4)


	DECLARE @CreateDateTimeColumn VARCHAR(128)
	DECLARE @CreateDateColumn VARCHAR(128)
	DECLARE @CreateUserColumn VARCHAR(128)
	DECLARE @CreateHostColumn VARCHAR(128)
	DECLARE @ModifyDateTimeColumn VARCHAR(128)
	DECLARE @ModifyDateColumn VARCHAR(128)
	DECLARE @ModifyUserColumn VARCHAR(128)
	DECLARE @ModifyHostColumn VARCHAR(128)
	
	SET @DatabaseName = DB_NAME()
	SET @SchemaName = 'stage'
	SET @TableName = 'DM_CLIENT'
	SET @Debug = 0
	SET @IndexFileGroup = '[INDEXES]'
	SET @DataCompression = 'PAGE'

	SET @CreateDateTimeColumn = 'Audit_Created_Datetime'
	SET @CreateDateColumn = 'Audit_Created_Date'
	SET @CreateUserColumn = 'Audit_Created_User'
	SET @CreateHostColumn = 'Audit_Created_Host'
	SET @ModifyDateTimeColumn = 'Audit_Modified_Datetime'
	SET @ModifyDateColumn = 'Audit_Modified_Date'
	SET @ModifyUserColumn = 'Audit_Modified_User'
	SET @ModifyHostColumn = 'Audit_Modified_Host'

	*/


	DECLARE @SQL NVARCHAR(MAX)
	DECLARE @CR VARCHAR(10)
	DECLARE @LF VARCHAR(10)
	DECLARE @CRLF VARCHAR(20)
	DECLARE @TAB VARCHAR(10)

	SET @CR = CHAR(10)
	SET @LF = CHAR(13)
	SET @CRLF = @CR + @LF
	SET @TAB = CHAR(9)

	IF OBJECT_ID('tempdb..#CreateDatabaseSQL') IS NOT NULL
		BEGIN
			DROP TABLE etlaudit.#CreateDatabaseSQL
		END

	CREATE TABLE etlaudit.#CreateDatabaseSQL
		(
			TEMP_ID INT IDENTITY(1, 1) NOT NULL,
			SQL NVARCHAR(MAX) NOT NULL
		)

	SET @SQL = 'SET NOCOUNT ON
' + @CRLF

	SET @SQL = @SQL + 'USE ' + @DatabaseName + '
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END

	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.triggers T WHERE name = ''trg_' + @TableName + '_UPDATE'' AND OBJECT_NAME(parent_id) = ''' + @TableName + ''' AND OBJECT_SCHEMA_NAME(parent_id) = ''' + @SchemaName + ''')
	BEGIN
		DROP TRIGGER ' + @SchemaName + '.trg_' + @TableName + '_UPDATE
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateDateTimeColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @CreateDateTimeColumn + ' DATETIME NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM ' + @SchemaName + '.' + @TableName + ' WHERE ' + @CreateDateTimeColumn + ' IS NULL)
	BEGIN
		UPDATE ' + @SchemaName + '.' + @TableName + ' SET ' + @CreateDateTimeColumn + ' = GETDATE()
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateDateTimeColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''' AND C.is_nullable = 1)
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ALTER COLUMN ' + @CreateDateTimeColumn + ' DATETIME NOT NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateDateColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @CreateDateColumn + ' AS CAST(' + @CreateDateTimeColumn + ' AS DATE)
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateUserColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @CreateUserColumn + ' NVARCHAR(64) NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM ' + @SchemaName + '.' + @TableName + ' WHERE ' + @CreateUserColumn + ' IS NULL)
	BEGIN
		UPDATE ' + @SchemaName + '.' + @TableName + ' SET ' + @CreateUserColumn + ' = SUSER_NAME()	
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateUserColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''' AND C.is_nullable = 1)
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ALTER COLUMN ' + @CreateUserColumn + ' NVARCHAR(64) NOT NULL
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateHostColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @CreateHostColumn + ' NVARCHAR(64) NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM ' + @SchemaName + '.' + @TableName + ' WHERE ' + @CreateHostColumn + ' IS NULL)
	BEGIN
		UPDATE ' + @SchemaName + '.' + @TableName + ' SET ' + @CreateHostColumn + ' = HOST_NAME()	
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateHostColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''' AND C.is_nullable = 1)
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ALTER COLUMN ' + @CreateHostColumn + ' NVARCHAR(64) NOT NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyDateTimeColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @ModifyDateTimeColumn + ' DATETIME NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyDateColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @ModifyDateColumn + ' AS CAST(' + @ModifyDateTimeColumn + ' AS DATE)
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyUserColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @ModifyUserColumn + ' NVARCHAR(64) NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyHostColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD ' + @ModifyHostColumn + ' NVARCHAR(64) NULL
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(N''' + @SchemaName + '.' +@TableName + ''') AND type = ''D'' AND name = ''DF_' + @TableName + '_' + @CreateDateTimeColumn + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD CONSTRAINT DF_' + @TableName + '_' + @CreateDateTimeColumn + ' DEFAULT GETDATE() FOR ' + @CreateDateTimeColumn + '
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(N''' + @SchemaName + '.' +@TableName + ''') AND type = ''D'' AND name = ''DF_' + @TableName + '_' + @CreateUserColumn + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD CONSTRAINT DF_' + @TableName + '_' + @CreateUserColumn + ' DEFAULT SUSER_NAME() FOR ' + @CreateUserColumn + '
	END
' + @CRLF

	
	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(N''' + @SchemaName + '.' +@TableName + ''') AND type = ''D'' AND name = ''DF_' + @TableName + '_' + @CreateHostColumn + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' ADD CONSTRAINT DF_' + @TableName + '_' + @CreateHostColumn + ' DEFAULT HOST_NAME() FOR ' + @CreateHostColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.indexes WHERE name = ''IX_' + @TableName + '_' + @CreateDateTimeColumn + ''' AND OBJECT_NAME(object_id) = ''' + @TableName + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		DROP INDEX ' + @SchemaName + '.' + @TableName + '.IX_' + @TableName + '_' + @CreateDateTimeColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = ''IX_' + @TableName + '_' + @CreateDateColumn + ''' AND OBJECT_NAME(object_id) = ''' + @TableName + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		CREATE NONCLUSTERED INDEX IX_' + @TableName + '_' + @CreateDateColumn + ' ON ' + @SchemaName + '.' + @TableName + '(' + @CreateDateColumn + ') WITH (FILLFACTOR = 85, PAD_INDEX = ON' + CASE WHEN @DataCompression IS NOT NULL THEN ', DATA_COMPRESSION = ' + @DataCompression ELSE '' END + ') ON ' + @IndexFileGroup + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''
	

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.triggers T WHERE name = ''trg_' + @TableName + '_UPDATE'' AND OBJECT_NAME(parent_id) = ''' + @TableName + ''' AND OBJECT_SCHEMA_NAME(parent_id) = ''' + @SchemaName + ''')
	BEGIN
		DROP TRIGGER ' + @SchemaName + '.trg_' + @TableName + '_UPDATE
	END
' + @CRLF


	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''


	SET @SQL = @SQL + 'CREATE TRIGGER trg_' + @TableName + '_UPDATE ON ' + @SchemaName + '.' + @TableName + '
FOR UPDATE AS            
BEGIN

	SET NOCOUNT ON

    UPDATE ' + @SchemaName + '.' + @TableName + '
    SET
		' + @ModifyDateTimeColumn + ' = GETDATE(),
		' + @ModifyUserColumn + ' = SUSER_NAME(),
		' + @ModifyHostColumn + ' = HOST_NAME(),
		' + @CreateDateTimeColumn + ' = D.' + @CreateDateTimeColumn + ',
		' + @CreateUserColumn + ' = D.' + @CreateUserColumn + ',
		' + @CreateHostColumn + ' = D.' + @CreateHostColumn + '
    FROM
		' + @SchemaName + '.' + @TableName + ' T
		INNER JOIN
		DELETED D ON
'

	DECLARE @PKColumnName VARCHAR(128)
	DECLARE @LoopCount INT

	-- Retrieves the PRIMARY KEY columns for the table
	DECLARE db_cursor CURSOR STATIC LOCAL
	FOR
	SELECT
		COL_NAME(ic.OBJECT_ID, ic.column_id) AS ColumnName
	FROM
		sys.indexes i
		INNER JOIN
		sys.index_columns ic ON
		i.OBJECT_ID = ic.OBJECT_ID AND
		i.index_id = ic.index_id
		INNER JOIN
		sys.tables t ON
		ic.object_id = t.object_id
		INNER JOIN
		sys.schemas s ON
		t.schema_id = s.schema_id
	WHERE
		i.is_primary_key = 1 AND
		OBJECT_NAME(ic.OBJECT_ID) = @TableName AND
		s.name = @SchemaName

	OPEN db_cursor   
	FETCH NEXT FROM db_cursor INTO @PKColumnName   

	SET @LoopCount = 1

	WHILE @@FETCH_STATUS = 0   
		BEGIN   
			-- PRINT @PKColumnName
			-- PRINT @@CURSOR_ROWS
			SET @SQL = @SQL + @TAB + @TAB + 'T.[' + @PKColumnName + '] = ' + 'D.[' + @PKColumnName + ']' + CASE WHEN @LoopCount = @@CURSOR_ROWS THEN '' + @CR ELSE ' AND' + @CR END

			SET @LoopCount = @LoopCount + 1
			FETCH NEXT FROM db_cursor INTO @PKColumnName   
		END   

	CLOSE db_cursor   
	DEALLOCATE db_cursor 

	SET @SQL = @SQL + 'END 
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	IF @Debug = 0
		BEGIN
			EXECUTE sys.sp_ExecuteSQL @SQL
		END
			
	SET @SQL = ''
	

	IF @Debug = 1
		BEGIN
			SELECT SQL FROM etlaudit.#CreateDatabaseSQL ORDER BY TEMP_ID
		END


	IF OBJECT_ID('tempdb..#CreateDatabaseSQL') IS NOT NULL
		BEGIN
			DROP TABLE etlaudit.#CreateDatabaseSQL
		END

END
GO
/****** Object:  StoredProcedure [etlaudit].[usp_AdminDropAuditingColumns]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_AdminDropAuditingColumns]
					@DatabaseName VARCHAR(128),
					@SchemaName	VARCHAR(128),
					@TableName VARCHAR(128),
					@Debug BIT = 0,
					@CreateDateTimeColumn VARCHAR(128) = 'Audit_Created_Datetime',
					@CreateDateColumn VARCHAR(128) = 'Audit_Created_Date',
					@CreateUserColumn VARCHAR(128) = 'Audit_Created_User',
					@CreateHostColumn VARCHAR(128) = 'Audit_Created_Host',
					@ModifyDateTimeColumn VARCHAR(128) = 'Audit_Modified_Datetime',
					@ModifyDateColumn VARCHAR(128) = 'Audit_Modified_Date',
					@ModifyUserColumn VARCHAR(128) = 'Audit_Modified_User',
					@ModifyHostColumn VARCHAR(128) = 'Audit_Modified_Host'
AS

BEGIN

	SET NOCOUNT ON
	
	/*
	-- Debugging Purposes

	DECLARE @DatabaseName VARCHAR(128)
	DECLARE @SchemaName VARCHAR(128)
	DECLARE @TableName VARCHAR(128)
	DECLARE @Debug BIT

	DECLARE @CreateDateTimeColumn VARCHAR(128)
	DECLARE @CreateDateColumn VARCHAR(128)
	DECLARE @CreateUserColumn VARCHAR(128)
	DECLARE @CreateHostColumn VARCHAR(128)
	DECLARE @ModifyDateTimeColumn VARCHAR(128)
	DECLARE @ModifyDateColumn VARCHAR(128)
	DECLARE @ModifyUserColumn VARCHAR(128)
	DECLARE @ModifyHostColumn VARCHAR(128)
	
	SET @DatabaseName = 'TRUDI'
	SET @SchemaName = 'dbo'
	SET @TableName = 'BusClass'
	SET @Debug = 1

	SET @CreateDateTimeColumn = 'Audit_Created_Datetime'
	SET @CreateDateColumn = 'Audit_Created_Date'
	SET @CreateUserColumn = 'Audit_Created_User'
	SET @CreateHostColumn = 'Audit_Created_Host'
	SET @ModifyDateTimeColumn = 'Audit_Modified_Datetime'
	SET @ModifyDateColumn = 'Audit_Modified_Date'
	SET @ModifyUserColumn = 'Audit_Modified_User'
	SET @ModifyHostColumn = 'Audit_Modified_Host'

	*/	

	DECLARE @SQL NVARCHAR(MAX)
	DECLARE @CR VARCHAR(10)
	DECLARE @LF VARCHAR(10)
	DECLARE @CRLF VARCHAR(20)
	DECLARE @TAB VARCHAR(10)

	SET @CR = CHAR(10)
	SET @LF = CHAR(13)
	SET @CRLF = @CR + @LF
	SET @TAB = CHAR(9)

	IF OBJECT_ID('tempdb..#CreateDatabaseSQL') IS NOT NULL
		BEGIN
			DROP TABLE etlaudit.#CreateDatabaseSQL
		END

	CREATE TABLE etlaudit.#CreateDatabaseSQL
		(
			TEMP_ID INT IDENTITY(1, 1) NOT NULL,
			SQL NVARCHAR(MAX) NOT NULL
		)

	SET @SQL = 'USE ' + @DatabaseName + '
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.triggers T WHERE name = ''trg_' + @TableName + '_UPDATE'' AND OBJECT_NAME(parent_id) = ''' + @TableName + ''' AND OBJECT_SCHEMA_NAME(parent_id) = ''' + @SchemaName + ''')
	BEGIN
		DROP TRIGGER ' + @SchemaName + '.trg_' + @TableName + '_UPDATE
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.indexes WHERE name = ''IX_' + @TableName + '_' + @CreateDateTimeColumn + ''' AND OBJECT_NAME(object_id) = ''' + @TableName + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		DROP INDEX ' + @SchemaName + '.' + @TableName + '.IX_' + @TableName + '_' + @CreateDateTimeColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.indexes WHERE name = ''IX_' + @TableName + '_' + @CreateDateColumn + ''' AND OBJECT_NAME(object_id) = ''' + @TableName + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		DROP INDEX ' + @SchemaName + '.' + @TableName + '.IX_' + @TableName + '_' + @CreateDateColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(N''' + @TableName + ''') AND type = ''D'' AND name = ''DF_' + @TableName + '_' + @CreateDateTimeColumn + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP CONSTRAINT DF_' + @TableName + '_' + @CreateDateTimeColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(N''' + @TableName + ''') AND type = ''D'' AND name = ''DF_' + @TableName + '_' + @CreateUserColumn + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP CONSTRAINT DF_' + @TableName + '_' + @CreateUserColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(N''' + @TableName + ''') AND type = ''D'' AND name = ''DF_' + @TableName + '_' + @CreateHostColumn + ''' AND OBJECT_SCHEMA_NAME(object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP CONSTRAINT DF_' + @TableName + '_' + @CreateHostColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''


	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateDateColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @CreateDateColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyDateColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @ModifyDateColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateDateTimeColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @CreateDateTimeColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyDateTimeColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @ModifyDateTimeColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateUserColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @CreateUserColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyUserColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @ModifyUserColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @CreateHostColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @CreateHostColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''

	SET @SQL = @SQL + 'IF EXISTS(SELECT * FROM sys.tables T INNER JOIN sys.columns C ON T.object_id = C.object_id AND C.name = ''' + @ModifyHostColumn + ''' AND T.name = ''' + @TableName + ''' AND T.type = ''U'' AND OBJECT_SCHEMA_NAME(T.object_id) = ''' + @SchemaName + ''')
	BEGIN
		ALTER TABLE ' + @SchemaName + '.' + @TableName + ' DROP COLUMN ' + @ModifyHostColumn + '
	END
' + @CRLF

	INSERT INTO etlaudit.#CreateDatabaseSQL(SQL) VALUES (@SQL)

	EXECUTE sys.sp_ExecuteSQL @SQL
			
	SET @SQL = ''
	

	IF @Debug = 1
		BEGIN
			SELECT @SQL = SQL FROM etlaudit.#CreateDatabaseSQL ORDER BY TEMP_ID
		END


	IF OBJECT_ID('tempdb..#CreateDatabaseSQL') IS NOT NULL
		BEGIN
			DROP TABLE etlaudit.#CreateDatabaseSQL
		END


END
GO
/****** Object:  StoredProcedure [etlaudit].[usp_GetConstantIntegerValue]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE  PROCEDURE [etlaudit].[usp_GetConstantIntegerValue]
	(
		@ConstantName	VARCHAR(100),
		@ConstantValue	INT OUTPUT
	) 
AS

--**********************************************************************************************
-- PROCEDURE:		usp_GetConstantIntegerValue
--
-- PARAMETERS:		@ConstantName - The name of the constant to look for in the constants meta table
--
-- APPLICATION:		
--
-- PURPOSE:			Return a constant value in integer format
-- NOTES:			None
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--
--**********************************************************************************************

DECLARE
	@StringConstantValue VARCHAR (100),
	@ErrNo INT

SET @ErrNo = 0

BEGIN TRY

	EXECUTE etlaudit.usp_GetConstantValue
									@ConstantName,
									@ConstantValue = @StringConstantValue OUTPUT

	IF (@StringConstantValue = 'NOT FOUND')
		BEGIN
			SET @ConstantValue = NULL
		END
	ELSE
		BEGIN
			SET @ConstantValue = CAST(@StringConstantValue AS INT)
		END

	RETURN @ErrNo

END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END
	--We will let the calling stored procedure handle this error.
	--If an error occurs the Error Number will not equal zero
	SET @ErrNo = ERROR_NUMBER()

	RETURN @ErrNo

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_GetConstantValue]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_GetConstantValue]
	(
		@ConstantName	VARCHAR(100),
		@ConstantValue	VARCHAR(100) OUTPUT
	) 
AS

--**********************************************************************************************
-- PROCEDURE:		usp_GetConstantValue
--
-- PARAMETERS:		@ConstantName - The name of the constant to look for in the constants meta table
--
-- APPLICATION:		
--
-- PURPOSE:			Return a constant value in varchar format
-- NOTES:			None
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--
--**********************************************************************************************

SET NOCOUNT ON

DECLARE @ErrNo INT
SET @ErrNo = 0

BEGIN TRY

	SELECT
		@ConstantValue = ConstantValue
	FROM
		etlaudit.ConstantsMeta
	WHERE
		ConstantName = @ConstantName

	IF ((@ConstantValue IS NULL) OR (@ConstantValue = ''))
		BEGIN
			SET @ConstantValue = 'NOT FOUND'
		END

	RETURN @ErrNo

END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END
	--We will let the calling stored procedure handle this error.
	--If an error occurs the Error Number will not equal zero
	SET @ErrNo = ERROR_NUMBER()

	RETURN @ErrNo

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_GetRowCount]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_GetRowCount]
	(
		@ProcessExecutionKey	INT,
		@DatabaseName VARCHAR(100),
		@SchemaName VARCHAR(50) = 'dbo',
		@TableName VARCHAR(100)
	)
AS
--**********************************************************************************************
-- PROCEDURE:		usp_GetRowCount
--
-- PARAMETERS:		@ProcessExecutionKey - The ProcessExecutionKey of the AuditPkgExecution table.
--					@TableName - The name of the dimension or fact table to get the row count.
--
-- APPLICATION:		
--
-- PURPOSE:			Get row count for a table
-- NOTES:			none
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--										
--**********************************************************************************************

SET NOCOUNT ON

DECLARE
	@NOT_FOUND VARCHAR(10),
	@ERROR_TYPE_SQL VARCHAR(100),
	@ERROR_TYPE_USER VARCHAR(100),
	@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT,
	@USER_DEFINED_SEVERITY_LEVEL_WARNING INT,
	@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT,
	@NOT_SETTLED VARCHAR(100),
	@SETTLED VARCHAR(100),
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DecisionPoint VARCHAR(255),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(100),
	@SQL AS NVARCHAR(1000)

--We are calling a seperate stored procedure that will be used to set constants that
--are used for error handling.  This stored procedure will need to handle it''s own 
--errors that occur within it to ensure that the following variables are set correctly 
--This will ensure reliable error handling for all stored procedures 
EXECUTE etlaudit.usp_SetErrorHandlingConstantValues 
												@ProcessExecutionKey = @ProcessExecutionKey,
												@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_WARNING = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL = @USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL OUTPUT,
												@NOT_FOUND = @NOT_FOUND OUTPUT,
												@ERROR_TYPE_SQL = @ERROR_TYPE_SQL OUTPUT,
												@ERROR_TYPE_USER = @ERROR_TYPE_USER OUTPUT,
												@SETTLED = @SETTLED OUTPUT,
												@NOT_SETTLED = @NOT_SETTLED OUTPUT


SET @ErrNo = ''
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DecisionPoint = ''
SET @TableColumn = ''
SET @SettledOrNotSettled = ''

BEGIN TRY

--*************************************************************************************************
-- SET CONSTANTS FOR STORED PROCEDURE
--*************************************************************************************************

	--Template for adding new constants.  Replace xxx with CONSTANT_NAME.
	/*
	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue
		''xxx'', @ConstantValue = @xxx OUTPUT
	IF (@xxx = @NOT_FOUND OR @ErrNo <> 0) 
	BEGIN
		SET @ErrMsg = ''usp_GetConstantValue xxx Lookup Failed''
		SET @ErrorType = @ERROR_TYPE_USER
		-- RAISERROR with severity 11-19 will cause execution to 
		-- jump to the CATCH block
		RAISERROR (
			@ErrMsg, -- Message text.
			@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD, -- Severity level for a constants violation.
			1 -- State.
		)
	END
	*/
	
--*************************************************************************************************
-- Procedure Body
--*************************************************************************************************

	SET @SQL = ''

	IF @DatabaseName IS NOT NULL
		BEGIN 
			SET @SQL = 'USE ' + @DatabaseName + CHAR(13) 
		END
	
	SET @DecisionPoint = 'Dynamically getting table row count'

	/*
	SET @SQL = @SQL + 
		'SELECT CAST(SUM(p.rows)AS INT) -- need to sum for partitioned tables
		FROM sys.tables t
		INNER JOIN sys.schemas s
			ON t.schema_id = s.schema_id 
		INNER JOIN sys.indexes i  
			ON i.OBJECT_ID = t.OBJECT_ID  
		INNER JOIN sys.partitions p
			ON i.OBJECT_ID = p.OBJECT_ID  
			AND i.index_id = p.index_id 
		WHERE i.index_id < 2  --indicates the clustered index, the first index
		AND t.type = ''U'' --user defined tables only
		AND t.name = ''' + @TableName + ''' 
		AND s.name = ''' + @SchemaName + ''''
	*/

	SET @SQL = @SQL +
		'
		USE ' + @DatabaseName + '
		
		SELECT
			SUM(row_count)
		FROM
			sys.dm_db_partition_stats
		WHERE
			object_id = OBJECT_ID(''' + @SchemaName + '.' + @TableName + ''') AND
			(
				index_id = 0
				OR
				index_id = 1
			)'

	EXECUTE sp_executesql @SQL
	
END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> @ERROR_TYPE_USER
		BEGIN
			SET @ErrorType = @ERROR_TYPE_SQL
		END

	--Most errors will not be settled.  In order to settle an error this variable needs to be 
	--set (SET @SettledOrNotSettled = @SETTLED).  If it is not set it is assummed it is an error 
	--that is not settled.
	IF @SettledOrNotSettled <> @SETTLED
		BEGIN
			SET @SettledOrNotSettled = @NOT_SETTLED
		END

    EXECUTE etlaudit.usp_LogAuditError
									@ProcessExecutionKey,
									@TableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType,
									@SettledOrNotSettled 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@TableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
			(
				@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
			)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_InsertAuditExtractionTime]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [etlaudit].[usp_InsertAuditExtractionTime]
	(
		@ProcessExecutionKey INT,
		@SourceSystem VARCHAR(50),
		@MinimumStartDatetime DATETIME = '1900-01-01',
		@ExtractionStartDatetime DATETIME OUTPUT,
		@ExtractionStopDatetime DATETIME OUTPUT,
		@ExtractionTimeKey INT OUTPUT
	)
AS

--**********************************************************************************************
-- PROCEDURE:		usp_InsertAuditExtractionTime
--
-- PARAMETERS:		@MinimumStartDate - This is the minimum date that you want for the extract of
--                  the source data if there no records in the etlaudit.AuditExtractTime table.
--
-- APPLICATION:		
--
-- PURPOSE:			Inserts into the etlaudit.AuditExtractionTime table and Retrieves the new source 
--					system's extract start and stop times.
--
-- NOTES:			none
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 								
--**********************************************************************************************

SET NOCOUNT ON

DECLARE
	@NOT_FOUND VARCHAR(10),
	@ERROR_TYPE_SQL VARCHAR(100),
	@ERROR_TYPE_USER VARCHAR(100),
	@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT,
	@USER_DEFINED_SEVERITY_LEVEL_WARNING INT,
	@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT,
	@NOT_SETTLED VARCHAR(100),
	@SETTLED VARCHAR(100),
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DimOrFactName VARCHAR(150),
	@DecisionPoint VARCHAR(255),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(100)

--We are calling a seperate stored procedure that will be used to set constants that
--are used for error handling.  This stored procedure will need to handle it's own 
--errors that occur within it to ensure that the following variables are set correctly 
--This will ensure reliable error handling for all stored procedures 
EXECUTE etlaudit.usp_SetErrorHandlingConstantValues 
												@ProcessExecutionKey = @ProcessExecutionKey,
												@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_WARNING = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL = @USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL OUTPUT,
												@NOT_FOUND = @NOT_FOUND OUTPUT,
												@ERROR_TYPE_SQL = @ERROR_TYPE_SQL OUTPUT,
												@ERROR_TYPE_USER = @ERROR_TYPE_USER OUTPUT,
												@SETTLED = @SETTLED OUTPUT,
												@NOT_SETTLED = @NOT_SETTLED OUTPUT


SET @ErrNo = ''
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DimOrFactName = 'etlaudit.AuditExtractionTime'
SET @DecisionPoint = ''
SET @TableColumn = ''
SET @SettledOrNotSettled = ''

BEGIN TRY


--*************************************************************************************************
-- SET CONSTANTS FOR STORED PROCEDURE
--*************************************************************************************************


	--Template for adding new constants.  Replace xxx with CONSTANT_NAME.
	/*
	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue
		'xxx', @ConstantValue = @xxx OUTPUT
	IF (@xxx = @NOT_FOUND OR @ErrNo <> 0) 
	BEGIN
		SET @ErrMsg = 'usp_GetConstantValue xxx Lookup Failed'
		SET @ErrorType = @ERROR_TYPE_USER
		-- RAISERROR with severity 11-19 will cause execution to 
		-- jump to the CATCH block
		RAISERROR (
			@ErrMsg, -- Message text.
			@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD, -- Severity level for a constants violation.
			1 -- State.
		)
	END
	*/
	
--*************************************************************************************************
-- Get Extract Start and Stop times
--*************************************************************************************************
	
	BEGIN TRANSACTION

		--Set the extract stop datetime to right now
		SET @ExtractionStopDatetime = GETDATE()

		--Get the last extraction stop date and set this as the new start date.
		--If the last extract start date is null then set it to the value of the @MinimumStartDate value

		IF NOT EXISTS(SELECT * FROM etlaudit.AuditExtractionTime WHERE SourceSystem = @SourceSystem)
			BEGIN
				SET @ExtractionStartDatetime = @MinimumStartDatetime
			END
		ELSE
			BEGIN
				SET @ExtractionStartDatetime = (
													SELECT
														--AET.ExtractStopDatetime
														CASE WHEN SourceSystem in ('Begin Pull Security Data', 'Begin Security Workflow')
															THEN 
																DATEADD(MINUTE, -10, AET.ExtractStopDatetime)
															ELSE
																DATEADD(MINUTE, -30, AET.ExtractStopDatetime)
															END 
													FROM
														(
															SELECT TOP 1
																AET.ExtractionTimeKey,
																MIN(APE.ProcessExecutionKey) AS ProcessExecutionKey
															FROM
																etlaudit.AuditExtractionTime AET
																INNER JOIN
																etlaudit.AuditProcessExecution APE ON
																AET.ExtractionTimeKey = APE.ExtractionTimeKey
															WHERE
																AET.SourceSystem = @SourceSystem
															GROUP BY
																AET.ExtractionTimeKey
															HAVING 
																MIN(ISNULL(APE.SuccessfulProcessingIndicator, 'N')) = 'Y'
															ORDER BY
																AET.ExtractionTimeKey DESC	
														) R
														INNER JOIN
														etlaudit.AuditExtractionTime AET ON
														R.ExtractionTimeKey = AET.ExtractionTimeKey
											   )
			END

		IF @ExtractionStartDatetime IS NULL
			BEGIN
				SET @ExtractionStartDatetime = @MinimumStartDatetime
			END

		--Now insert the values into the AuditExtractionTime and return the key as well as the start and stop dates
		INSERT INTO etlaudit.AuditExtractionTime
			(
				ExtractStartDatetime,
				ExtractStopDatetime,
				SourceSystem
			)
		VALUES
			(
				@ExtractionStartDatetime,
				@ExtractionStopDatetime,
				@SourceSystem
			)

		SET @ExtractionTimeKey = SCOPE_IDENTITY()

		--Now Update the AuditProcessExecution Table and set the ExtractionTimeKey
		UPDATE etlaudit.AuditProcessExecution
		SET ExtractionTimeKey = @ExtractionTimeKey
		WHERE
			ProcessExecutionKey = @ProcessExecutionKey

	COMMIT TRANSACTION

END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> @ERROR_TYPE_USER
		BEGIN
			SET @ErrorType = @ERROR_TYPE_SQL
		END

	--Most errors will not be settled.  In order to settle an error this variable needs to be 
	--set (SET @SettledOrNotSettled = @SETTLED).  If it is not set it is assummed it is an error 
	--that is not settled.
	IF @SettledOrNotSettled <> @SETTLED
		BEGIN
			SET @SettledOrNotSettled = @NOT_SETTLED
		END

    EXECUTE etlaudit.usp_LogAuditError
									@ProcessExecutionKey,
									@DimOrFactName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType,
									@SettledOrNotSettled 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@DimOrFactName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
			(
				@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
			)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_InsertAuditProcessExecution]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_InsertAuditProcessExecution]
													@ProcessName VARCHAR (150) = NULL,
													@ExecutionStartDatetime DATETIME = NULL,
													@ExecutionStopDatetime DATETIME = NULL,
													@ParentProcessExecutionKey INT = -1,
													@ProcessExecutionKey INT OUTPUT,
													@ExtractionTimeKey INT = -1
AS
--**********************************************************************************************
-- PROCEDURE:		usp_InsertAuditProcessExecution
--
-- PARAMETERS:		@ProcessName - The Name of the package
--					
--
-- APPLICATION:		
--
-- PURPOSE:			Insert a new record into the Audit Package Execution table
-- NOTES:			none
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 									
--										
--**********************************************************************************************
SET NOCOUNT ON

SET @ProcessExecutionKey = -1

DECLARE
	@NOT_FOUND VARCHAR(10),
	@ERROR_TYPE_SQL VARCHAR(100),
	@ERROR_TYPE_USER VARCHAR(100),
	@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT,
	@USER_DEFINED_SEVERITY_LEVEL_WARNING INT,
	@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT,
	@NOT_SETTLED VARCHAR(100),
	@SETTLED VARCHAR(100),
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DimOrFactName VARCHAR(150),
	@DecisionPoint VARCHAR(255),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(100)
	
SET @ErrNo = NULL

--We are calling a seperate stored procedure that will be used to set constants that
--are used for error handling.  This stored procedure will need to handle it's own 
--errors that occur within it to ensure that the following variables are set correctly 
--This will ensure reliable error handling for all stored procedures 
EXECUTE etlaudit.usp_SetErrorHandlingConstantValues 
												@ProcessExecutionKey = @ProcessExecutionKey,
												@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_WARNING = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL = @USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL OUTPUT,
												@NOT_FOUND = @NOT_FOUND OUTPUT,
												@ERROR_TYPE_SQL = @ERROR_TYPE_SQL OUTPUT,
												@ERROR_TYPE_USER = @ERROR_TYPE_USER OUTPUT,
												@SETTLED = @SETTLED OUTPUT,
												@NOT_SETTLED = @NOT_SETTLED OUTPUT


SET @ErrNo = ''
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DimOrFactName = 'AuditProcessExecution'
SET @DecisionPoint = ''
SET @TableColumn = ''
SET @SettledOrNotSettled = ''

DECLARE @Datetime	DATETIME

SET @Datetime = GETDATE()

BEGIN TRY

	INSERT INTO etlaudit.AuditProcessExecution
		(
			ProcessName,
			ExecutionStartDatetime, 
			ParentProcessExecutionKey,
			ExtractionTimeKey
		)
	VALUES
		(
			@ProcessName, 
			@Datetime,
			@ParentProcessExecutionKey,
			@ExtractionTimeKey
		)

	SET @ProcessExecutionKey = SCOPE_IDENTITY()

	-- Update the ParentProcessExecutionKey column to the @ProcessExecutionKey
	-- This will make this package the parent package
	IF @ParentProcessExecutionKey = -1
		BEGIN
			UPDATE etlaudit.AuditProcessExecution
			SET ParentProcessExecutionKey = @ProcessExecutionKey
			WHERE
				ProcessExecutionKey = @ProcessExecutionKey
		END

END TRY
BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT <> 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> @ERROR_TYPE_USER
		BEGIN
			SET @ErrorType = @ERROR_TYPE_SQL
		END

	--Most errors will not be settled.  In order to settle an error this variable needs to be 
	--set (SET @SettledOrNotSettled = @SETTLED).  If it is not set it is assummed it is an error 
	--that is not settled.
	IF @SettledOrNotSettled <> @SETTLED
		BEGIN
			SET @SettledOrNotSettled = @NOT_SETTLED
		END

    EXECUTE etlaudit.usp_LogAuditError
									@ProcessExecutionKey,
									@DimOrFactName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType,
									@SettledOrNotSettled 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@DimOrFactName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 


    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
			(
				@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
			)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_InsertAuditRow]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_InsertAuditRow]
	(
		@ProcessExecutionKey INT = 0,
		@DataFileKey INT = 0,
		@TableName VARCHAR(100) = NULL,
		@TableProcessKey INT = 0,
		@BranchName VARCHAR(150) = NULL,
		@ProcessingSummaryGroup VARCHAR(150) = NULL,
		@AuditKey INT OUTPUT
	)
AS
--**********************************************************************************************
-- PROCEDURE:		usp_InsertAuditRow
--
-- PARAMETERS:		@ProcessExecutionKey - The Process Execution Key.
--					@TableName - The table name of the dimension or fact.
--					@TableProcessKey - The Table Process Key.
--					@BranchName - Name supplied by the ETL developer for the branch that 
--					inserts or updates data in the target table.
--					@ProcessingSummaryGroup - When we summarize rowcounts in AuditTableProcessing, 
--					is this branch considered "standard" or "nonstandard"?
--					@AuditKey - The output parameter that will be used to return the AuditKey.
--
-- APPLICATION:		
--
-- PURPOSE:			The purpose of this stored procedure is to insert a new record into the table
--					Audit and return the Audit Key so it can be used to load the dimensions.
--
-- NOTES:			none
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--**********************************************************************************************

SET NOCOUNT ON

DECLARE
	@NOT_FOUND VARCHAR(10),
	@ERROR_TYPE_SQL VARCHAR(100),
	@ERROR_TYPE_USER VARCHAR(100),
	@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT,
	@USER_DEFINED_SEVERITY_LEVEL_WARNING INT,
	@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT,
	@NOT_SETTLED VARCHAR(100),
	@SETTLED VARCHAR(100),
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DecisionPoint VARCHAR(255),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(100)

--We are calling a seperate stored procedure that will be used to set constants that
--are used for error handling.  This stored procedure will need to handle it's own 
--errors that occur within it to ensure that the following variables are set correctly 
--This will ensure reliable error handling for all stored procedures 
EXECUTE etlaudit.usp_SetErrorHandlingConstantValues 
												@ProcessExecutionKey = @ProcessExecutionKey,
												@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_WARNING = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL = @USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL OUTPUT,
												@NOT_FOUND = @NOT_FOUND OUTPUT,
												@ERROR_TYPE_SQL = @ERROR_TYPE_SQL OUTPUT,
												@ERROR_TYPE_USER = @ERROR_TYPE_USER OUTPUT,
												@SETTLED = @SETTLED OUTPUT,
												@NOT_SETTLED = @NOT_SETTLED OUTPUT


SET @ErrNo = ''
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DecisionPoint = ''
SET @TableColumn = ''
SET @SettledOrNotSettled = ''

BEGIN TRY

--*************************************************************************************************
-- SET CONSTANTS FOR STORED PROCEDURE
--*************************************************************************************************

	--Template for adding new constants.  Replace xxx with CONSTANT_NAME.
	/*
	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue
		'xxx', @ConstantValue = @xxx OUTPUT
	IF (@xxx = @NOT_FOUND OR @ErrNo <> 0) 
	BEGIN
		SET @ErrMsg = 'usp_GetConstantValue xxx Lookup Failed'
		SET @ErrorType = @ERROR_TYPE_USER
		-- RAISERROR with severity 11-19 will cause execution to 
		-- jump to the CATCH block
		RAISERROR (
			@ErrMsg, -- Message text.
			@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD, -- Severity level for a constants violation.
			1 -- State.
		)
	END
	*/
	
	BEGIN TRANSACTION

		--The Audit dimension keeps track of each execution branch of your Integration 
		--Services packages. By properly maintaining the audit structure, you can determine 
		--which package -- and which branch of each package -- loaded (or updated) data in 
		--the data warehouse tables. And, when that package was executed.
		
		SET @DecisionPoint = 'Inserting into etlaudit.Audit'

		INSERT INTO etlaudit.Audit
			(
				TableProcessKey,
				DataFileKey,
				BranchName, 
				ProcessingSummaryGroup
			)
		VALUES
			(
				@TableProcessKey, 
				@DataFileKey,
				@BranchName, 
				@ProcessingSummaryGroup
			)

		SET @AuditKey = SCOPE_IDENTITY()

	COMMIT TRANSACTION

END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> @ERROR_TYPE_USER
		BEGIN
			SET @ErrorType = @ERROR_TYPE_SQL
		END

	--Most errors will not be settled.  In order to settle an error this variable needs to be 
	--set (SET @SettledOrNotSettled = @SETTLED).  If it is not set it is assummed it is an error 
	--that is not settled.
	IF @SettledOrNotSettled <> @SETTLED
		BEGIN
			SET @SettledOrNotSettled = @NOT_SETTLED
		END

    EXECUTE etlaudit.usp_LogAuditError
									@ProcessExecutionKey,
									@TableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType,
									@SettledOrNotSettled 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@TableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
			(
				@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
			)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_InsertAuditTableProcessing]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_InsertAuditTableProcessing]
	(
		@ProcessExecutionKey INT = 0, 
		@SourceTableName VARCHAR(100),
		@SourceDatabase VARCHAR(100),
		@DestinationTableName VARCHAR(100),
		@DestinationDatabase VARCHAR(100),
		@RowCount INT = 0,
		@TableProcessKey INT OUTPUT
	)
AS
--**********************************************************************************************
-- PROCEDURE:		usp_InsertAuditTableProcessing
--
-- PARAMETERS:		@ProcessExecutionKey - Package Execution Key.
--					@TableName - The name of the table we are processing.
--					@RowCount - The initial rowcount of the table we are processing.
--					@TableProcessKey - The ouput parameter of this stored procedure.
--
-- APPLICATION:		
--
-- PURPOSE:			The purpose of this stored procedure is to insert a new record into the table
--					AuditTableProcessing and return the Table Process Key.
--
-- NOTES:			none
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--**********************************************************************************************

SET NOCOUNT ON

DECLARE
	@NOT_FOUND VARCHAR(10),
	@ERROR_TYPE_SQL VARCHAR(100),
	@ERROR_TYPE_USER VARCHAR(100),
	@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT,
	@USER_DEFINED_SEVERITY_LEVEL_WARNING INT,
	@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT,
	@NOT_SETTLED VARCHAR(100),
	@SETTLED VARCHAR(100),
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DecisionPoint VARCHAR(255),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(100)

--We are calling a seperate stored procedure that will be used to set constants that
--are used for error handling.  This stored procedure will need to handle it's own 
--errors that occur within it to ensure that the following variables are set correctly 
--This will ensure reliable error handling for all stored procedures 
EXECUTE etlaudit.usp_SetErrorHandlingConstantValues 
												@ProcessExecutionKey = @ProcessExecutionKey,
												@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_WARNING = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL = @USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL OUTPUT,
												@NOT_FOUND = @NOT_FOUND OUTPUT,
												@ERROR_TYPE_SQL = @ERROR_TYPE_SQL OUTPUT,
												@ERROR_TYPE_USER = @ERROR_TYPE_USER OUTPUT,
												@SETTLED = @SETTLED OUTPUT,
												@NOT_SETTLED = @NOT_SETTLED OUTPUT


SET @ErrNo = ''
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DecisionPoint = ''
SET @TableColumn = ''
SET @SettledOrNotSettled = ''

BEGIN TRY

--*************************************************************************************************
-- SET CONSTANTS FOR STORED PROCEDURE
--*************************************************************************************************

	--Template for adding new constants.  Replace xxx with CONSTANT_NAME.
	/*
	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue
		'xxx', @ConstantValue = @xxx OUTPUT
	IF (@xxx = @NOT_FOUND OR @ErrNo <> 0) 
	BEGIN
		SET @ErrMsg = 'usp_GetConstantValue xxx Lookup Failed'
		SET @ErrorType = @ERROR_TYPE_USER
		-- RAISERROR with severity 11-19 will cause execution to 
		-- jump to the CATCH block
		RAISERROR (
			@ErrMsg, -- Message text.
			@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD, -- Severity level for a constants violation.
			1 -- State.
		)
	END
	*/

	BEGIN TRANSACTION

		-- We want to assign a TableProcessKey for each table that
		-- gets new rows during the execution of this package. In 
		-- many cases, there's only one TableProcessKey per package,
		-- because most packages touch only one table. But especially
		-- during Fact table processing, it's common to have one 
		-- package insert a few rows into a second table, usually 
		-- dimension tables to ensure referential integrity. In that case,
		-- you'd set up two Get TableProcessKey tasks to track them
		-- separately.

		SET @DecisionPoint = 'Inserting into AuditTableProcessing'
		
		INSERT INTO etlaudit.AuditTableProcessing
			(
				ProcessExecutionKey, 
				SourceTableName,
				SourceDatabase,
				DestinationTableName,
				DestinationDatabase,
				TableInitialRowCnt
			)
		VALUES
			(
				@ProcessExecutionKey, 
				@SourceTableName,
				@SourceDatabase,
				@DestinationTableName,
				@DestinationDatabase,
				@RowCount
			)

		SET @TableProcessKey = SCOPE_IDENTITY()

	COMMIT TRANSACTION

END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> @ERROR_TYPE_USER
		BEGIN
			SET @ErrorType = @ERROR_TYPE_SQL
		END

	--Most errors will not be settled.  In order to settle an error this variable needs to be 
	--set (SET @SettledOrNotSettled = @SETTLED).  If it is not set it is assummed it is an error 
	--that is not settled.
	IF @SettledOrNotSettled <> @SETTLED
		BEGIN
			SET @SettledOrNotSettled = @NOT_SETTLED
		END

    EXECUTE etlaudit.usp_LogAuditError
									@ProcessExecutionKey,
									@DestinationTableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType,
									@SettledOrNotSettled 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@DestinationTableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
			(
				@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
			)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_LogAuditError]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_LogAuditError]
	(
		@ProcessExecutionKey INT = NULL,
		@DimOrFactName VARCHAR(150) = NULL,
		@DecisionPoint VARCHAR(255) = NULL,
		@TableColumn VARCHAR(80) = NULL,
		@ErrorType VARCHAR(20) = NULL,
		@SettledNotSettled VARCHAR(11) = NULL,
		@AuditErrorLogKey int = 0 OUTPUT -- contains the AuditErrorLogKey of the row inserted by usp_LogAuditError in the AuditErrorLog table
	)
AS

--**********************************************************************************************
-- PROCEDURE:		usp_LogAuditError
--
-- PARAMETERS:		None
--
-- APPLICATION:		
--
-- PURPOSE:			Error Logging
-- NOTES:			Logs error information in the AuditErrorLog table about the 
--					error that caused execution to jump to the CATCH block of a 
--					TRY...CATCH construct. This should be executed from within the scope 
--					of a CATCH block otherwise it will return without inserting error 
--					information. 
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--**********************************************************************************************

BEGIN
    
	SET NOCOUNT ON

    -- Output parameter value of 0 indicates that error 
    -- information was not logged
    SET @AuditErrorLogKey = 0;

    BEGIN TRY

        -- Return if there is no error information to log
        IF ERROR_NUMBER() IS NULL
            RETURN;

        -- Return if inside an uncommittable transaction.
        -- Data insertion/modification is not allowed when 
        -- a transaction is in an uncommittable state.
        IF XACT_STATE() = -1
        BEGIN
            PRINT 'Cannot log error since the current transaction is in an uncommittable state. ' 
                + 'Rollback the transaction before executing usp_LogAuditError in order to successfully log error information.';
            RETURN;
        END

		--Insert error into AuditErrorLog
		INSERT INTO etlaudit.AuditErrorLog
			(
				ProcessExecutionKey,
				DimOrFactName,
				UserName,
				ErrorDate,
				ErrorProcedure,
				ErrorLine,
				DecisionPoint,
				ErrorMessage,
				ErrorNumber,
				ErrorSeverity,
				ErrorState,
				ErrorType,
				TableColumn,
				SettledFlag
			)
		VALUES
			( 
				@ProcessExecutionKey,
				@DimOrFactName,
				CONVERT(sysname, SUSER_NAME()),
				GETDATE(),
				ERROR_PROCEDURE(),
				ERROR_LINE(),
				@DecisionPoint,
				ERROR_MESSAGE(),
				ERROR_NUMBER(),
				ERROR_SEVERITY(),
				ERROR_STATE(),
				@ErrorType,
				@TableColumn,
				@SettledNotSettled
			)

        -- Pass back the AuditErrorLogID of the row inserted
        SET @AuditErrorLogKey = SCOPE_IDENTITY()

    END TRY

    BEGIN CATCH

        PRINT 'An error occurred in stored procedure usp_LogAuditError: ';
		--To turn off/on Debugging modify the variable DEBUG in the following 
		--stored procedure
		EXECUTE etlaudit.usp_PrintAuditError
										@ProcessExecutionKey,
										@DimOrFactName,
										@DecisionPoint,
										@TableColumn,
										@ErrorType 
        RETURN -1;

    END CATCH
END;
GO
/****** Object:  StoredProcedure [etlaudit].[usp_PrintAuditError]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_PrintAuditError]
	(
		@PkgExecKey INT = NULL,
		@DimOrFactName VARCHAR(150) = NULL,
		@DecisionPoint VARCHAR(255) = NULL,
		@TableColumn VARCHAR(80) = NULL,
		@ErrorType VARCHAR(20) = NULL
	)
AS

--**********************************************************************************************
-- PROCEDURE:		usp_PrintAuditError
--
-- PARAMETERS:		None
--
-- APPLICATION:		
--
-- PURPOSE:			Prints Error Information
-- NOTES:			usp_PrintAuditError prints error information about the error that caused 
--					execution to jump to the CATCH block of a TRY...CATCH block. 
--					Should be executed from within the scope of a CATCH block otherwise 
--					it will return without printing any error information.
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--
--**********************************************************************************************

SET NOCOUNT ON

DECLARE @DEBUG VARCHAR(3)

SET @DEBUG = 'OFF'

IF @DEBUG = 'ON'
	BEGIN

		-- Print error information. 
		PRINT 'Error ' + CONVERT(varchar(50), ERROR_NUMBER()) +
			  ', Severity ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
			  ', State ' + CONVERT(varchar(5), ERROR_STATE()) + 
			  ', Procedure ' + ISNULL(ERROR_PROCEDURE(), '-') + 
			  ', Line ' + CONVERT(varchar(5), ERROR_LINE());
		PRINT ERROR_MESSAGE();
		PRINT 'PkgExecKey: ' + CONVERT(VARCHAR(30), ISNULL(@PkgExecKey, 0)) + 
				', Dimension or Fact name: ' + ISNULL(@DimOrFactName, '') +
				', DecisionPoint: ' + ISNULL(@DecisionPoint, '') +
				', TableColumn: ' + ISNULL(@TableColumn, '') +
				', ErrorType: ' + ISNULL(@ErrorType, '')

	END
GO
/****** Object:  StoredProcedure [etlaudit].[usp_SetErrorHandlingConstantValues]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_SetErrorHandlingConstantValues] 
	(
		@ProcessExecutionKey INT,
		@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT OUTPUT,
		@USER_DEFINED_SEVERITY_LEVEL_WARNING INT OUTPUT,
		@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT OUTPUT,
		@NOT_FOUND VARCHAR(10) OUTPUT,
		@ERROR_TYPE_SQL VARCHAR(100) OUTPUT,
		@ERROR_TYPE_USER VARCHAR(100) OUTPUT,
		@SETTLED VARCHAR(100) OUTPUT,
		@NOT_SETTLED VARCHAR(100) OUTPUT
	)
AS

--**********************************************************************************************
-- PROCEDURE:		usp_SetErrorHandlingConstantValues
--
-- PARAMETERS:		None
--
-- APPLICATION:		
--
-- PURPOSE:			Set the constant values for all variables that are used for error handling
-- NOTES:			None
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--
--**********************************************************************************************

DECLARE
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DimOrFactName VARCHAR(150),
	@DecisionPoint VARCHAR(255),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(11)

SET @NOT_FOUND = 'NOT FOUND'
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DimOrFactName = 'N/A'
SET @DecisionPoint = ''
SET @TableColumn = ''
SET @SettledOrNotSettled = ''
SET @ErrNo = 0

BEGIN TRY

	SET @DecisionPoint = 'Setting Error Handling Constants'

	EXECUTE @ErrNo = etlaudit.usp_GetConstantIntegerValue @ConstantName = 'USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD', @ConstantValue = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT

	IF (@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD IS NULL OR @ErrNo <> 0) 
		BEGIN
			SET @ErrMsg = 'usp_SetErrorHandlingConstantValues USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD Lookup Failed'
			SET @ErrorType = 'User Defined Error'
			-- RAISERROR with severity 11-19 will cause execution to 
			-- jump to the CATCH block
			RAISERROR
				(
					@ErrMsg, -- Message text.
					11, -- We will hard code this value in case the user defined security level is not found
					1 -- We will hard code this value until we lookup the user defined state.
				)
		END

	EXECUTE @ErrNo = etlaudit.usp_GetConstantIntegerValue @ConstantName = 'USER_DEFINED_SEVERITY_LEVEL_WARNING', @ConstantValue = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT
	
	IF (@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD IS NULL OR @ErrNo <> 0) 
		BEGIN
			SET @ErrMsg = 'usp_SetErrorHandlingConstantValues USER_DEFINED_SEVERITY_LEVEL_WARNING Lookup Failed'
			SET @ErrorType = 'User Defined Error'
			-- RAISERROR with severity 11-19 will cause execution to 
			-- jump to the CATCH block
			RAISERROR
				(
					@ErrMsg, -- Message text.
					11, -- We will hard code this value in case the user defined security level is not found
					1 -- We will hard code this value until we lookup the user defined state.
				)
		END

	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue @ConstantName = 'ERROR_TYPE_SQL', @ConstantValue = @ERROR_TYPE_SQL OUTPUT

	IF (@ERROR_TYPE_SQL = @NOT_FOUND OR @ErrNo <> 0) 
		BEGIN
			SET @ErrMsg = 'usp_SetErrorHandlingConstantValues ERROR_TYPE_SQL Lookup Failed'
			SET @ErrorType = 'User Defined Error'
			-- RAISERROR with severity 11-19 will cause execution to 
			-- jump to the CATCH block
			RAISERROR
					(
						@ErrMsg, -- Message text.
						11, -- We will hard code this value in case the user defined security level is not found
						1 -- We will hard code this value until we lookup the user defined state.
					)
		END

	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue @ConstantName = 'ERROR_TYPE_USER', @ConstantValue = @ERROR_TYPE_USER OUTPUT

	IF (@ERROR_TYPE_USER = @NOT_FOUND OR @ErrNo <> 0) 
		BEGIN
			SET @ErrMsg = 'usp_SetErrorHandlingConstantValues ERROR_TYPE_USER Lookup Failed'
			SET @ErrorType = 'User Defined Error'
			-- RAISERROR with severity 11-19 will cause execution to 
			-- jump to the CATCH block
			RAISERROR
					(
						@ErrMsg, -- Message text.
						11, -- We will hard code this value in case the user defined security level is not found
						1 -- We will hard code this value until we lookup the user defined state.
					)
		END

	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue @ConstantName = 'NOT_SETTLED', @ConstantValue = @NOT_SETTLED OUTPUT

	IF (@NOT_SETTLED = @NOT_FOUND OR @ErrNo <> 0) 
		BEGIN
			SET @ErrMsg = 'usp_SetErrorHandlingConstantValues NOT_SETTLED Lookup Failed'
			SET @ErrorType = 'User Defined Error'
			-- RAISERROR with severity 11-19 will cause execution to 
			-- jump to the CATCH block
			RAISERROR
					(
						@ErrMsg, -- Message text.
						11, -- We will hard code this value in case the user defined security level is not found
						1 -- We will hard code this value until we lookup the user defined state.
					)
		END

	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue @ConstantName = 'SETTLED', @ConstantValue = @SETTLED OUTPUT

	IF (@SETTLED = @NOT_FOUND OR @ErrNo <> 0) 
		BEGIN
			SET @ErrMsg = 'usp_SetErrorHandlingConstantValues SETTLED Lookup Failed'
			SET @ErrorType = 'User Defined Error'
			-- RAISERROR with severity 11-19 will cause execution to 
			-- jump to the CATCH block
			RAISERROR
					(
						@ErrMsg, -- Message text.
						11, -- We will hard code this value in case the user defined security level is not found
						1 -- We will hard code this value until we lookup the user defined state.
					)
		END
--	RETURN @ErrNo

END TRY
-- This is a special case for an error handler.  In most cases constants are used to 
-- Update the error log table.  But since we can't rely on the constants being set
-- the values are hard coded here.  This is the only spot in the entire ETL auditing
-- where hard codes will be used.

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> 'User Defined Error'
		BEGIN
			SET @ErrorType = 'SQL Server Defined Error'
		END


    EXECUTE etlaudit.usp_LogAuditError
								@ProcessExecutionKey,
								@DimOrFactName,
								@DecisionPoint,
								@TableColumn,
								@ErrorType,
								'Not Settled' 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@DimOrFactName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 


    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
		(
			@ErrorMessage, -- Message text.
            @ErrorSeverity, -- Severity.
            @ErrorState -- State.
		)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_UpdateAudit]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_UpdateAudit] 
	(
		@ProcessExecutionKey INT,
		@TableName VARCHAR(150),
		@AuditKey INT,
		@BranchRowCnt INT
	)
AS
--**********************************************************************************************
-- PROCEDURE:		usp_UpdateAudit
--
-- PARAMETERS:		@ProcessExecutionKey - The Package Execution Key of the AuditPkgExecution table.
--					@TableName - The table name of the table being inserted into or updated.
--					@AuditKey - The audit key of the Audit Dimension.
--					@BranchRowCnt - The number of rows updated or inserted.
--
-- APPLICATION:		
--
-- PURPOSE:			The purpose of this stored procedure is to update all relavent meta information
--					for an update or insert into the audit dimension
--
-- NOTES:			none
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--**********************************************************************************************

SET NOCOUNT ON

DECLARE
	@NOT_FOUND VARCHAR(10),
	@ERROR_TYPE_SQL VARCHAR(100),
	@ERROR_TYPE_USER VARCHAR(100),
	@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT,
	@USER_DEFINED_SEVERITY_LEVEL_WARNING INT,
	@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT,
	@NOT_SETTLED VARCHAR(100),
	@SETTLED VARCHAR(100),
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DecisionPoint VARCHAR(50),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(100)

--We are calling a seperate stored procedure that will be used to set constants that
--are used for error handling.  This stored procedure will need to handle it's own 
--errors that occur within it to ensure that the following variables are set correctly 
--This will ensure reliable error handling for all stored procedures 
EXECUTE etlaudit.usp_SetErrorHandlingConstantValues 
												@ProcessExecutionKey = @ProcessExecutionKey,
												@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_WARNING = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL = @USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL OUTPUT,
												@NOT_FOUND = @NOT_FOUND OUTPUT,
												@ERROR_TYPE_SQL = @ERROR_TYPE_SQL OUTPUT,
												@ERROR_TYPE_USER = @ERROR_TYPE_USER OUTPUT,
												@SETTLED = @SETTLED OUTPUT,
												@NOT_SETTLED = @NOT_SETTLED OUTPUT


SET @ErrNo = ''
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DecisionPoint = ''
SET @TableColumn = ''
SET @SettledOrNotSettled = ''

--*************************************************************************************************
-- SET CONSTANTS FOR STORED PROCEDURE
--*************************************************************************************************


	--Template for adding new constants.  Replace xxx with CONSTANT_NAME.
	/*
	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue
		'xxx', @ConstantValue = @xxx OUTPUT
	IF (@xxx = @NOT_FOUND OR @ErrNo <> 0) 
	BEGIN
		SET @ErrMsg = 'usp_GetConstantValue xxx Lookup Failed'
		SET @ErrorType = @ERROR_TYPE_USER
		-- RAISERROR with severity 11-19 will cause execution to 
		-- jump to the CATCH block
		RAISERROR (
			@ErrMsg, -- Message text.
			@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD, -- Severity level for a constants violation.
			1 -- State.
		)
	END
	*/

--*************************************************************************************************
-- Update Audit
--*************************************************************************************************
BEGIN TRY
	
	SET @DecisionPoint = 'Updating etlaudit.Audit'

	UPDATE etlaudit.Audit
	SET BranchRowCnt = @BranchRowCnt
	WHERE 
		AuditKey = @AuditKey

END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> @ERROR_TYPE_USER
		BEGIN
			SET @ErrorType = @ERROR_TYPE_SQL
		END

	--Most errors will not be settled.  In order to settle an error this variable needs to be 
	--set (SET @SettledOrNotSettled = @SETTLED).  If it is not set it is assummed it is an error 
	--that is not settled.
	IF @SettledOrNotSettled <> @SETTLED
		BEGIN
			SET @SettledOrNotSettled = @NOT_SETTLED
		END

    EXECUTE etlaudit.usp_LogAuditError
									@ProcessExecutionKey,
									@TableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType,
									@SettledOrNotSettled 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@TableName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
			(
				@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
			)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_UpdateAuditProcessExecution]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [etlaudit].[usp_UpdateAuditProcessExecution]
	(
		@ProcessExecutionKey INT,
		@SuccessfulProcessingIndicator CHAR(1) = 'Y'
	) 
AS
--**********************************************************************************************
-- PROCEDURE:		usp_UpdateAuditProcessExecution
--
-- PARAMETERS:		@ProcessExecutionKey - The id to the AuditPkgExecution table
--
-- APPLICATION:		
--
-- PURPOSE:			Update the AuditProcessExecution table
-- NOTES:			none
--
-- CREATED:			Dan Gilmore	2021-05-18
--
-- MODIFIED 
-- DATE				AUTHOR				DESCRIPTION
-- ----------		--------------		--------------------------------------------------------
-- 2021-05-18		Dan Gilmore			Initial release.
-- 
--**********************************************************************************************

SET NOCOUNT ON

DECLARE
	@NOT_FOUND VARCHAR(10),
	@ERROR_TYPE_SQL VARCHAR(100),
	@ERROR_TYPE_USER VARCHAR(100),
	@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD INT,
	@USER_DEFINED_SEVERITY_LEVEL_WARNING INT,
	@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL INT,
	@NOT_SETTLED VARCHAR(100),
	@SETTLED VARCHAR(100),
	@ErrNo INT,
	@ErrMsg VARCHAR(255),
	@ErrorType VARCHAR(20),
	@DimOrFactName VARCHAR(150),
	@DecisionPoint VARCHAR(255),
	@TableColumn VARCHAR(80),
	@SettledOrNotSettled VARCHAR(100)

--We are calling a seperate stored procedure that will be used to set constants that
--are used for error handling.  This stored procedure will need to handle it's own 
--errors that occur within it to ensure that the following variables are set correctly 
--This will ensure reliable error handling for all stored procedures 
EXECUTE etlaudit.usp_SetErrorHandlingConstantValues 
												@ProcessExecutionKey = @ProcessExecutionKey,
												@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD = @USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_WARNING = @USER_DEFINED_SEVERITY_LEVEL_WARNING OUTPUT,
												@USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL = @USER_DEFINED_SEVERITY_LEVEL_INFORMATIONAL OUTPUT,
												@NOT_FOUND = @NOT_FOUND OUTPUT,
												@ERROR_TYPE_SQL = @ERROR_TYPE_SQL OUTPUT,
												@ERROR_TYPE_USER = @ERROR_TYPE_USER OUTPUT,
												@SETTLED = @SETTLED OUTPUT,
												@NOT_SETTLED = @NOT_SETTLED OUTPUT

SET @ErrNo = ''
SET @ErrMsg = ''
SET @ErrorType = ''
SET @DecisionPoint = ''
SET @DimOrFactName = 'etlaudit.AuditProcessExecution'
SET @TableColumn = ''
SET @SettledOrNotSettled = ''

--*************************************************************************************************
-- SET CONSTANTS FOR STORED PROCEDURE
--*************************************************************************************************


	--Template for adding new constants.  Replace xxx with CONSTANT_NAME.
	/*
	EXECUTE @ErrNo = etlaudit.usp_GetConstantValue
		'xxx', @ConstantValue = @xxx OUTPUT
	IF (@xxx = @NOT_FOUND OR @ErrNo <> 0) 
	BEGIN
		SET @ErrMsg = 'usp_GetConstantValue xxx Lookup Failed'
		SET @ErrorType = @ERROR_TYPE_USER
		-- RAISERROR with severity 11-19 will cause execution to 
		-- jump to the CATCH block
		RAISERROR (
			@ErrMsg, -- Message text.
			@USER_DEFINED_SEVERITY_LEVEL_STOP_LOAD, -- Severity level for a constants violation.
			1 -- State.
		)
	END
	*/

--*************************************************************************************************
-- Update AuditPkgExecution
--*************************************************************************************************
BEGIN TRY

	SET @DecisionPoint = 'Updating etlaudit.AuditProcessExecution'

	UPDATE etlaudit.AuditProcessExecution
	SET
		ExecutionStopDatetime = GETDATE(), 
		SuccessfulProcessingIndicator = @SuccessfulProcessingIndicator  
	WHERE 
		ProcessExecutionKey = @ProcessExecutionKey

END TRY

BEGIN CATCH
    -- Rollback any active or uncommittable transactions before
    -- inserting information in the ErrorLog
    IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

	--Set the error type.  If this is not user generated then set SQL Server generated it.
	IF @ErrorType <> @ERROR_TYPE_USER
		BEGIN
			SET @ErrorType = @ERROR_TYPE_SQL
		END

	--Most errors will not be settled.  In order to settle an error this variable needs to be 
	--set (SET @SettledOrNotSettled = @SETTLED).  If it is not set it is assummed it is an error 
	--that is not settled.
	IF @SettledOrNotSettled <> @SETTLED
		BEGIN
			SET @SettledOrNotSettled = @NOT_SETTLED
		END

    EXECUTE etlaudit.usp_LogAuditError
									@ProcessExecutionKey,
									@DimOrFactName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType,
									@SettledOrNotSettled 

	--To turn off/on Debugging modify the variable DEBUG in the following 
	--stored procedure
    EXECUTE etlaudit.usp_PrintAuditError
									@ProcessExecutionKey,
									@DimOrFactName,
									@DecisionPoint,
									@TableColumn,
									@ErrorType 

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    DECLARE @ErrorMessage NVARCHAR(4000)
    DECLARE @ErrorSeverity INT
    DECLARE @ErrorState INT

    SELECT 
        @ErrorMessage = ERROR_MESSAGE(),
        @ErrorSeverity = ERROR_SEVERITY(),
        @ErrorState = ERROR_STATE()

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR
			(
				@ErrorMessage, -- Message text.
				@ErrorSeverity, -- Severity.
				@ErrorState -- State.
			)

END CATCH
GO
/****** Object:  StoredProcedure [etlaudit].[usp_updateStats]    Script Date: 4/23/2024 10:49:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--=======================================================================================
--  Modification History
--  Author            Date          Comment
--
--  Darpan Desai   06/09/22      Initial Script
--=======================================================================================

CREATE PROCEDURE [etlaudit].[usp_updateStats] AS

BEGIN

    -- Insert statements for procedure here
    UPDATE STATISTICS etlaudit.AuditExtractionTime;
	UPDATE STATISTICS etlaudit.AuditProcessExecution;

END
GO
