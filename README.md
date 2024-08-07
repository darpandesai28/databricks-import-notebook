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



{"reportName":"DeviceInstallStatusByApp","filter":'(ApplicationId eq ' + f"'{app_id}'" + ')'}
      url = f"https://graph.microsoft.us/beta/deviceManagement/reports/exportJobs/"   

body['filter'] = '(ApplicationId eq ' + f"'{app_id}'" + ')'
    url = f"https://graph.microsoft.us/beta/deviceManagement/reports/getDeviceInstallStatusReport" 


    df = spark.read \
          .format("com.databricks.spark.sqldw") \
          .option("url", "jdbc:sqlserver://synwseitaasdatateam.sql.azuresynapse.usgovcloudapi.net:1433;database=synwsdbdatateam;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.usgovcloudapi.net;loginTimeout=30") \
          .option("tempDir", "wasbs://" + data_lake_container + "@" + data_lake_endpoint + "/tempDirs") \
          .option("forwardSparkAzureStorageCredentials", "true") \
          .option("enableServicePrincipalAuth", "true") \
          .option("dbTable", "darpan.dateDim") \
          .option("tempDir", "wasbs://" + data_lake_container + "@" + data_lake_endpoint + "/tempDirs") \
          .load()  
display(df)   



df.write \
  .format("com.databricks.spark.sqldw") \
  .mode("append") \
  .option("url", jdbc_connection_string + ";encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.usgovcloudapi.net;loginTimeout=30") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("enableServicePrincipalAuth", "true") \
  .option("dbTable", destination_database_name + "." + destination_table_name) \
  .option("tempDir", "wasbs://" + data_lake_container + "@" + data_lake_endpoint + "/tempDirs") \
  .option("tableOptions","HEAP,DISTRIBUTION = ROUND_ROBIN") \
  .save()


  # Defining a separate set of service principal credentials for Azure Synapse Analytics (If not defined, the connector will use the Azure storage account credentials)  
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.id", service_principal_id)  
spark.conf.set("spark.databricks.sqldw.jdbc.service.principal.client.secret", service_principal_key)  
