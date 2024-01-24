# Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

- [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
- [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
- Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns
Select dD.*,t.* from
(SELECT distinct serialNumber

  FROM [servicenow].[snCmdbCiComputer] ccc
  WHERE	ccc.installStatus = 1 
				  AND ccc.upcfunction = 'eud_customer'
				  AND ISNULL(ccc.assignedtoId, '00000000-0000-0000-0000-000000000000') <> '00000000-0000-0000-0000-000000000000') as t 
Left outer join (select distinct deviceName,serialNumber from dbo.deviceDim where isCurrentYN = 1 and deviceName like 'AFC%') dD
on t.serialNumber = dD.serialNumber order by 1 desc
