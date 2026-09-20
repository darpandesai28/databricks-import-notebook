# Databricks notebook source
# MAGIC %run "Common/Configuration"

# COMMAND ----------

from cde_functions import *

# COMMAND ----------

dbutils.widgets.text("StartDateTime","")
StartDateTime = dbutils.widgets.get("StartDateTime")

dbutils.widgets.text("EndDateTime","")
EndDateTime = dbutils.widgets.get("EndDateTime")

# COMMAND ----------

bearer_token = get_bearer_token("https://api.loganalytics.us",tenant_id,service_principal_id,service_principal_key)
process_time = datetime.now()

# COMMAND ----------

folder_path_date = process_time.strftime("%Y/%m/%d/%H/")
file_name_datetime = process_time.strftime("-%Y-%m-%d-%H-%M") 

file_name = "vpn-diagnostic-log"
container_name = "vpn-diagnostic-log"
destination_table_name = "stage.vpndiagnosticlog"

response_file_name = f"{folder_path_date}{file_name}{file_name_datetime}.json"

response_blob_client = get_blob_client_sp(tenant_id,service_principal_id,service_principal_key,blob_storage_name,container_name,response_file_name)

# COMMAND ----------

# DBTITLE 1,Check for Prod Vs. non-Prod Environment
clusterName = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")

if clusterName == 'pvaeitaascdecore-adf' :
  tableName = 'AzureDiagnostics'
else:
  tableName = 'VPNLogData_CL'

# COMMAND ----------

response_str = ""
retry_count = 0
query = f"""{tableName} | where TimeGenerated between (todatetime('{StartDateTime}')..todatetime('{EndDateTime}')) and Category in ("GatewayDiagnosticLog","TunnelDiagnosticLog","RouteDiagnosticLog","IKEDiagnosticLog","P2SDiagnosticLog")
| project ResourceId,Category,OperationName,TimeGenerated,Level,instance_s,operationStatus_s,Message,
  configuration_GatewayMode_s,configuration_GatewaySku_s,configuration_VIPAddress_s,   configuration_CAs_GatewayTenantWorker_IN_0_s,configuration_CAs_GatewayTenantWorker_IN_1_s,
configuration_VirtualNetworkRanges_s,configuration_VPNClientAddressPool_s,configuration_VPNClientProtocol_s,
configuration_ClientRootCerts_s,configuration_DnsAddresses_s,configuration_BgpConfiguration_GatewayConfig_PeerAddress_s,configuration_BgpConfiguration_GatewayConfig_Asn_d,configuration_BgpConfiguration_GatewayConfig_PeerType_s,
configuration_EnableBgpRouteTranslationForNat_b,configuration_VPNClientEnableInternetSecurity_b,ClientOperationId_g,CorrelationRequestId_g | sort by TimeGenerated asc"""

url = f"https://api.loganalytics.us/v1/workspaces/{log_analytics_workspace_id}/query?query={query}"

#Make REST API call to Log Analytics 
response = get_api(bearer_token,url)

if "error" in response:
  raise ValueError(response)
#Use json.dumps to convert the Python dictionary into a JSON string
jsonData = json.dumps(response)

#Add the json content to a list
jsonDataList = []
jsonDataList.append(jsonData)

#Convert the list to a RDD and parse it using spark.read.json
jsonRDD = sc.parallelize(jsonDataList)
df = spark.read.json(jsonRDD)   

# COMMAND ----------

# DBTITLE 1,Parse Columns and Rows into Key-Value pair
from pyspark.sql.types import StringType, DateType, StructType, BooleanType, ArrayType, StructField
from pyspark.sql.functions import *

df = df.withColumn("_tables", explode(df["tables"]))
df = df.withColumn("_rows", explode(df["_tables.rows"]))

if not bool(df.head(1)) :
    dbutils.notebook.exit(0)
else:
 df = df.select(df["_rows"][0].alias("ResourceId")
              ,df["_rows"][1].alias("Category")
              ,df["_rows"][2].alias("OperationName")
              ,df["_rows"][3].cast('timestamp').alias("TimeGenerated")
              ,df["_rows"][4].alias("Level")
              ,df["_rows"][5].alias("Instance")
              ,df["_rows"][6].alias("OperationStatus")
              ,df["_rows"][7].alias("Message")
              ,df["_rows"][8].alias("GatewayMode")
              ,df["_rows"][9].alias("GatewaySku")
              ,df["_rows"][10].alias("VIPAddress")
              ,df["_rows"][11].alias("GatewayTenantWorker_IN_0")
              ,df["_rows"][12].alias("GatewayTenantWorker_IN_1")
              ,df["_rows"][13].alias("VirtualNetworkRanges")
              ,df["_rows"][14].alias("VPNClientAddressPool")
              ,df["_rows"][15].alias("VPNClientProtocol")
              ,df["_rows"][16].alias("ClientRootCerts")
              ,df["_rows"][17].alias("DnsAddresses")
              ,df["_rows"][18].alias("PeerAddress")
              ,df["_rows"][19].alias("Asn")
              ,df["_rows"][20].alias("PeerType")
              ,df["_rows"][21].cast('boolean').alias("EnableBgpRouteTranslationForNat")
              ,df["_rows"][22].cast('boolean').alias("VPNClientEnableInternetSecurity")
              ,df["_rows"][23].alias("ClientOperationId")
              ,df["_rows"][24].alias("CorrelationRequestId")            
              )
 df = df.withColumn("filePath",lit(response_file_name))
 df = df.withColumn("fileCreatedDate", current_date())
 df = df.withColumn("fileCreatedDateTime", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Write to Blob Storage Account
import json

df_panda = df.toPandas()

df_panda[[ 'TimeGenerated', 'fileCreatedDate','fileCreatedDateTime' ]] = df_panda[['TimeGenerated', 'fileCreatedDate','fileCreatedDateTime']].astype(str)

result = df_panda.to_json(orient= "records")
parsed = json.loads(result)
jsonData = json.dumps(parsed)

retry_count = 0
retry_threshold = 5
    
while(retry_count < retry_threshold):
  try:
    print("Response Write Attempt #" + str(retry_count))
    response_blob_client.upload_blob(data=jsonData)
    retry_count = retry_threshold
  except Exception as failure:
    print(failure.args)
    retry_count +=1
    time.sleep(30)
    if retry_count >= retry_threshold:
      raise ValueError(json.dumps(failure.args))

# COMMAND ----------

df.write \
      .format("com.databricks.spark.sqldw") \
      .mode("append") \
      .option("url", jdbc_connection_string + ";encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.usgovcloudapi.net;loginTimeout=30") \
      .option("enableServicePrincipalAuth", "true") \
      .option("dbTable",destination_table_name) \
      .option("tempDir", "abfss://" + data_lake_container + "@" + data_lake_name + ".dfs.core.usgovcloudapi.net/tempDirs") \
      .option("tableOptions","HEAP,DISTRIBUTION = ROUND_ROBIN") \
      .save()  
