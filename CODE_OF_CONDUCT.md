# Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

- [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
- [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
- Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns
case when coalesce(time_worked,'') = '' then 0 else round(((CAST(to_timestamp(time_worked) as BIGINT)) - (CAST(to_timestamp('1970-01-01 00:00:00') as BIGINT)))/60) end as  incidentDurationNumber,
          case when coalesce(calendar_duration,'') = '' then 0 else round(((CAST(to_timestamp(calendar_duration) as BIGINT)) - (CAST(to_timestamp('1970-01-01 00:00:00') as BIGINT)))/60) end as totalIncidentDurationNumber,
