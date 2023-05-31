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
    
    metricName
ActionLatency
ActionsCompleted
ActionsFailed
ActionsStarted
ActionsSucceeded
ActionSuccessLatency
Active Cores
Active Nodes
active_connections
ActivityCancelledRuns
ActivityFailedRuns
ActivitySucceededRuns
allocated_data_storage
AppConnections
ArpAvailability
AutoscaleMaxThroughput
Availability
AvailableStorage
AverageBandwidth
AverageMemoryWorkingSet
AverageResponseTime
AvgRequestCountPerHealthyHost
backup_storage_used
BgpAvailability
BillableActionExecutions
BillableTriggerExecutions
BillingUsageNativeOperation
BillingUsageStandardConnector
BillingUsageStorageConsumption
BitsInPerSecond
BitsOutPerSecond
blocked_by_firewall
ByteCount
BytesReceived
BytesReceivedRate
BytesSent
BytesSentRate
Cancel Requested Runs
Cancelled Runs
Completed Runs
connection_failed
connection_successful
connections_failed
cpu_limit
cpu_percent
cpu_used
CpuMemoryCapacityMegabytes
CpuMemoryUtilizationMegabytes
CpuPercentage
CpuTime
CpuUtilizationPercentage
CurrentAssemblies
DataUsage
diff_backup_size_bytes
DiskQueueLength
DiskReadMegabytes
DiskUsedMegabytes
DiskWriteMegabytes
DocumentCount
DocumentQuota
dtu_consumption_percent
dtu_limit
dtu_used
Egress
Errors
ExpressRouteGatewayActiveFlows
ExpressRouteGatewayBitsPerSecond
ExpressRouteGatewayCountOfRoutesAdvertisedToPeer
ExpressRouteGatewayCountOfRoutesLearnedFromPeer
ExpressRouteGatewayCpuUtilization
ExpressRouteGatewayFrequencyOfRoutesChanged
ExpressRouteGatewayMaxFlowsCreationRate
ExpressRouteGatewayNumberOfVmInVnet
ExpressRouteGatewayPacketsPerSecond
FactorySizeInGbUnits
Failed Runs
FileSystemUsage
Finalizing Runs
full_backup_size_bytes
FunctionExecutionCount
FunctionExecutionUnits
Gen0Collections
Gen1Collections
Gen2Collections
Handles
Http101
Http2xx
Http3xx
Http401
Http403
Http404
Http406
Http4xx
Http5xx
HttpQueueLength
HttpResponseTime
HybridWorkerPing
Idle Cores
Idle Nodes
IndexUsage
Ingress
io_consumption_percent
IoOtherBytesPerSecond
IoOtherOperationsPerSecond
IoReadBytesPerSecond
IoReadOperationsPerSecond
IoWriteBytesPerSecond
IoWriteOperationsPerSecond
Leaving Cores
Leaving Nodes
log_backup_size_bytes
log_write_percent
MaxAllowedFactorySizeInGbUnits
MaxAllowedResourceCount
memory_percent
MemoryPercentage
MemoryWorkingSet
MetadataRequests
network_bytes_egress
network_bytes_ingress
NetworkInputMegabytes
NetworkOutputMegabytes
NormalizedRUConsumption
P2SBandwidth
P2SConnectionCount
PacketCount
PacketsReceivedRate
PacketsSentRate
physical_data_read_percent
PipelineCancelledRuns
PipelineElapsedTimeRuns
PipelineFailedRuns
PipelineSucceededRuns
Preempted Cores
Preempted Nodes
Preparing Runs
PrivateBytes
ProvisionedThroughput
Queued Runs
Quota Utilization Percentage
Requests
ResourceCount
RunFailurePercentage
RunLatency
RunsCompleted
RunsFailed
RunsStarted
serverlog_storage_limit
serverlog_storage_percent
serverlog_storage_usage
ServerSideLatency
ServiceApiHit
ServiceApiLatency
ServiceApiResult
ServiceAvailability
sessions_percent
SocketInboundAll
SocketLoopback
SocketOutboundAll
SocketOutboundEstablished
SocketOutboundTimeWait
sqlserver_process_core_percent
sqlserver_process_memory_percent
Started Runs
Starting Runs
storage
storage_limit
storage_percent
storage_used
SuccessE2ELatency
SuccessServerLatency
SynCount
TcpCloseWait
TcpClosing
TcpEstablished
TcpFinWait1
TcpFinWait2
TcpLastAck
TcpSynReceived
TcpSynSent
TcpTimeWait
tempdb_data_size
tempdb_log_size
tempdb_log_used_percent
Threads
Total Cores
TotalAppDomains
TotalAppDomainsUnloaded
TotalBillableExecutions
TotalJob
TotalRequests
TotalRequestUnits
TotalUpdateDeploymentMachineRuns
TotalUpdateDeploymentRuns
Transactions
TriggerCancelledRuns
TriggerFailedRuns
TriggerFireLatency
TriggerLatency
TriggersCompleted
TriggersFired
TriggersStarted
TriggersSucceeded
TriggerSucceededRuns
TriggerSuccessLatency
Unusable Cores
Unusable Nodes
VipAvailability
Warnings
workers_percent
xtp_storage_percent
![image](https://github.com/darpandesai28/databricks-import-notebook/assets/39281473/c3e088c3-35a0-48c9-8b1a-711eb57b6c5f)

