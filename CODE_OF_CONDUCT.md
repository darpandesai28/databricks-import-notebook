# Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

- [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
- [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
- Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns


%sql
select distinct c.assetId,a.assetId from
(Select distinct serialNumber, assetId,assignedToId from serviceNow_stagedata.sncmdbCiComputer where installStatus = 1 and UPPER(uPcFunction) = 'EUD_CUSTOMER' and assignedToId <> '00000000-0000-0000-0000-000000000000' and companyId = '317b75b6-74a6-9010-9c55-e168314b90e4') c 
inner join 
(Select case when length(RTRIM(LTRIM(model_category_value))) = 0 OR coalesce(model_category_value,'') = '' then '00000000-0000-0000-0000-000000000000' 
               when length(RTRIM(LTRIM(model_category_value))) < 32 then '00000000-0000-0000-0000-000000000000' else
               concat(substring(RTRIM(LTRIM(model_category_value)),1,8),'-',
                      substring(RTRIM(LTRIM(model_category_value)),9,4),'-',
                      substring(RTRIM(LTRIM(model_category_value)),13,4),'-',
                      substring(RTRIM(LTRIM(model_category_value)),17,4),'-',
                      substring(RTRIM(LTRIM(model_category_value)),21,length(model_category_value))) end as modelCategoryId,case when length(RTRIM(LTRIM(sys_id))) = 0 OR coalesce(sys_id,'') = '' then '00000000-0000-0000-0000-000000000000' 
               when length(RTRIM(LTRIM(sys_id))) < 32 then '00000000-0000-0000-0000-000000000000' else
               concat(substring(RTRIM(LTRIM(sys_id)),1,8),'-',
                      substring(RTRIM(LTRIM(sys_id)),9,4),'-',
                      substring(RTRIM(LTRIM(sys_id)),13,4),'-',
                      substring(RTRIM(LTRIM(sys_id)),17,4),'-',
                      substring(RTRIM(LTRIM(sys_id)),21,length(sys_id))) end as assetId
from (Select * from serviceNow_rawdata.almasset
                 LATERAL VIEW json_tuple(model_category, 'value') as model_category_value) a
                 where 
model_category_value = '81feb9c137101000deeabfc8bcbe5dc4') a
on c.assetId = a.assetId

if object_id('tempdb.dbo.#afct') is not null
	drop table #afct

/api/now/table/<tablename>?sysparm_limit=10&sysparm_offset=0


Then for next 10 records, you would simply increment the value of sysparm_offset by the value of your sysparm_limit

/api/now/table/tablename?sysparm_limit=10&sysparm_offset=10

/api/now/table/tablename?sysparm_limit=10&sysparm_offset=20

/api/now/table/tablename?sysparm_limit=10&sysparm_offset=30

.

.

/api/now/table/tablename?sysparm_limit=10&sysparm_offset=n
