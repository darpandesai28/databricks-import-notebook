# Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

- [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
- [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
- Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns


SELECT *
FROM sys.dm_pdw_exec_requests
WHERE [status] NOT IN ('Completed','Failed','Cancelled')
AND session_id <> session_id()
-- AND [label] = '<YourLabel>'
-- AND resource_allocation_percentage is not NULL
ORDER BY submit_time DESC;
 
-- Find top 10 longest running queries
SELECT TOP 10 *
FROM sys.dm_pdw_exec_requests
ORDER BY total_elapsed_time DESC;
