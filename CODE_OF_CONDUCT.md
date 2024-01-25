# Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

- [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
- [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
- Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns

with cte 
as (Select * ,
ROW_NUMBER() OVER(PARTITION BY serialNumber order by LatestDate desc) as RNum
from
(select serialNumber,deviceName,whenCreated,whenUpdated,
case when whenUpdated > whenCreated then whenUpdated else  whenCreated end as LatestDate

        from dbo.deviceDim where serialNumber <> 'N/A') as dD) 

Select t.serialNumber,cte.deviceName
 from
(SELECT distinct serialNumber

  FROM [servicenow].[snCmdbCiComputer] ccc
  WHERE	ccc.installStatus = 1 
				  AND ccc.upcfunction = 'eud_customer'
				  AND ISNULL(ccc.assignedtoId, '00000000-0000-0000-0000-000000000000') <> '00000000-0000-0000-0000-000000000000') as t 
Left outer join (Select * from cte where RNum = 1) cte
on t.serialNumber = cte.serialNumber order by 2 desc
--13 Missing
