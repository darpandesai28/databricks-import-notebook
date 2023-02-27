/****** Object:  Table [dbo].[WeekLookup]    Script Date: 2/27/2023 3:36:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[WeekLookup](
	[RowNum] [bigint] NULL,
	[WeekStartDate] [datetime] NULL,
	[WeekEndDate] [datetime] NULL,
	[flag] [int] NOT NULL
) ON [PRIMARY]
GO
