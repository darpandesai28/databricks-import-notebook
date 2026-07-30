# Databricks Import Directory Action

GitHub Action that imports the files from a local path into a Databricks workspace. [![Build](https://github.com/microsoft/databricks-import-notebook/actions/workflows/cd.yml/badge.svg)](https://github.com/microsoft/databricks-import-notebook/actions/workflows/cd.yml)

## When to use

This action is useful when you need to import a directory to the Databricks workspace, for example, when you want to import notebooks into a specific path.
Only directories and files with the extensions .scala, .py, .sql, .r, .R, .ipynb are imported. 

## How it works

The GitHub Action works with the 'import_dir' command from the Databricks Workspace Cli

## Getting Started

### Prerequisites

* Make sure you have a directory in your repo you want to import into Databricks
* Make sure you have installed the [Databricks Cli](https://github.com/marketplace/actions/install-databricks-cli)
* Make sure you have a Databricks Access Token. It can be a [PAT](https://docs.databricks.com/dev-tools/api/latest/authentication.html), or if you're working with Azure Databricks, it can be an [AAD Token](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token).

>Note: You can find both sample workflows in this repository.

### Usage

```yml
steps:
    - name: databricks-import-directory
      uses: microsoft/databricks-import-directory@v1.0.0
      with:
        databricks-host: https://<instance-name>.cloud.databricks.com
        databricks-token: token
        local-path: ./my-local-path
        remote-path: /my-remote-path
```

### Inputs

| Name | Description | Required | Default value |
| --- | --- | --- | --- |
| `databricks-host` | Workspace URL, with the format https://<instance-name>.cloud.databricks.com | true |NA|
| `databricks-token` | Databricks token, it can be a PAT or an AAD Token | true |NA|
| `local-path` | Path of the directory you want to import | true |NA|
| `remote-path` | Path of the directory inside Databricks workspace where you want the files to land| true |NA|

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.

1. Begin A365 ActiveUserLicense Workflow
2. Begin A365 OfficeActivation Workflow
3. Begin A365 Workflow
4. Begin ACAS Nessus Scans Workflow
5. Begin ACAS Scans Workflow
6. Begin ACAS Tenable Scans Workflow
7. Begin ADOC RFC Active Workflow
8. Begin ADOC RFC Archive Workflow
9. Begin ADOC SIR Workflow
10. Begin AFC User Workflow
11. Begin Append Blob Archival
12. Begin Append Blob Archival_Databricks
13. Begin Append Blob Archival_Databricks_New_Version
14. Begin Append Blob Archival_Databricks_v1
15. Begin AVD Log Workflow Adhoc
16. Begin Call Session Workflow
17. Begin Device Management Workflow
18. Begin Diagnostic Data Workflow
19. Begin Download Call Sessions
20. Begin Ingest GFEBS
21. Begin MDE Workflow
22. Begin Metric Pull Data
23. Begin OrgHierarchy Workflow
24. Begin Policy Workflow
25. Begin Power BI 4h Workflow
26. Begin Prod Data Copy Workflow_Old_Databricks
27. Begin Purview Endpoint Workload Activities Workflow
28. Begin Purview Exchange Workload Activities Workflow
29. Begin Purview OneDrive Workload Activities Workflow
30. Begin Purview PowerBI Workload Activities Workflow
31. Begin Purview SharePoint Workload Activities Workflow
32. Begin Purview Workload Activities Workflow
33. Begin Purview Workload Activities Workflow_old
34. Begin QA Workflow
35. Begin RCC-C Incident Workflow
36. Begin RCC-C Request Workflow
37. Begin Restart Databricks Environment
38. Begin Security Incident Update Workflow
39. Begin ServiceNow Workflow
40. Begin Shutdown All Environments
41. Begin Spectrum Workflow
42. Begin STACO Workflow
43. Begin Start All Environments
44. Begin Survey Workflow
45. Begin Teams Workflow
