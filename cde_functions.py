import time, re, json, os, uuid, pytz, requests, pandas as pd, pyspark, azure, sys, requests, msal
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, wait 
from datetime import datetime, timedelta, tzinfo
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__, ResourceTypes, AccountSasPermissions, generate_account_sas
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient
from requests.structures import CaseInsensitiveDict
from requests.models import Response
from socket import error as SocketError

class cde_functions():
  # Split String
  def split_str(mystr):
    try:
      temp_arr = mystr.split('},')
      temp_arr = [i.strip() for i in temp_arr]
      temp_arr[0] = temp_arr[0].split('[')[1]
      temp_arr[-1] = temp_arr[-1].split(']')[0]
      temp_str = "}|".join(temp_arr)
      return temp_str.split('|')
    except:
      return []
  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Build Structure
  def build_struct(jsstr):
    temp_arr = jsstr.split(',')
    temp_arr[0] = temp_arr[0].split('{')[1]
    temp_arr[-1] = temp_arr[-1].split('}')[0]
    mydict = {}; count = 0
    for arr in temp_arr:
      new_arr = arr.split(":")
      if count == 1:
        mydict["entityValueName"] = new_arr[0].strip()[1:-1]
        mydict["entityValue"] = new_arr[1].strip()[1:-1]
      else:
        mydict[new_arr[0].strip()[1:-1]] = new_arr[1].strip()[1:-1]
      count += 1
    return mydict

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Is Mounted?
  def ismounted(dbutils,path):
    if any(mount.mountPoint == path for mount in dbutils.fs.mounts()):
      index = path.rfind('/', 0, len(path) - 1)
      folder_name = path[index + 1:]
      mnt = dbutils.fs.ls(path[0:index + 1])

      for FileInfo in mnt:
        if FileInfo.name == folder_name:
          return True
    return False

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Mount
  def mount(dbutils,abfss_path,storage_path,mount_configs):
    if (not ismounted(dbutils,storage_path)):
      dbutils.fs.mount(
        source = abfss_path,
        mount_point = storage_path,
        extra_configs = mount_configs)

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Unmount
  def unmount(dbutils,mount_path,folder=None):  
    if folder is not None:
      if folder.strip() == "":
        print("Provide a folder name")
        return
      mount_path = mount_path + "/" + folder
    dbutils.fs.unmount(mount_path)
    dbutils.fs.rm(mount_path, True)

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Remove HTML
  def remove_html(text):
    tag_re = re.compile(r'<[^>]+>')
    text.replace("</div>", "\n\r</div>") # Keep newline characters
    return tag_re.sub('', text)

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Parse communication Field
  def parse_communication_into_json (text):
    text = remove_html(text)
    text = str.replace(text, '\"', '\\"')
    text = str.replace(text, '\n', '')
    text = str.replace(text, '\r', '')
    text = text if 'SUMMARY OF IMPACT: ' in text.upper() else 'SUMMARY OF IMPACT: ' + text
    text = re.sub('SUMMARY OF IMPACT: ', '","SummaryOfImpact":\"', text, flags=re.IGNORECASE)
    text = re.sub('PRELIMINARY ROOT CAUSE: ', '","RootCause":\"', text, flags=re.IGNORECASE)
    text = re.sub('Root Cause: ', '","RootCause":\"', text, flags=re.IGNORECASE)
    text = re.sub('Mitigation: ', '","Mitigation":\"', text, flags=re.IGNORECASE)
    text = re.sub('Current Status: ',  '","CurrentStatus":\"', text, flags=re.IGNORECASE)
    text = re.sub('Next Steps: ',  '","NextSteps":\"', text, flags=re.IGNORECASE)
    text = '{\"' + text + '\"}'
    text = str.replace(text, '{\"\",\"',  '{\"')
    text = str.replace(text, '&nbsp;',  '')
    return text

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Get API Bearer Token
  def get_bearer_token_v2(scope,tenant_id,service_principal_id,service_principal_key,timeout=90):
    url = "https://login.microsoftonline.us/" + tenant_id + "/oauth2/v2.0/token"
    
    data = CaseInsensitiveDict()
    data["grant_type"] = "client_credentials"
    data["client_id"] = service_principal_id
    data["client_secret"] = service_principal_key
    data["scope"] = scope
    
    response = requests.get(url, data=data, timeout=timeout)
    return json.loads(response.text)['access_token']

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Get API Bearer Token
  def get_bearer_token(resource,tenant_id,service_principal_id,service_principal_key,timeout=90):
    url = "https://login.microsoftonline.us/" + tenant_id + "/oauth2/token"
    
    data = CaseInsensitiveDict()
    data["grant_type"] = "client_credentials"
    data["client_id"] = service_principal_id
    data["client_secret"] = service_principal_key
    data["resource"] = resource
    
    response = requests.get(url, data=data, timeout=timeout)
    return json.loads(response.text)['access_token']

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Get SharePoint API Bearer Token
  def get_sharepoint_bearer_token(sharepoint_auth_config):
    app = msal.ConfidentialClientApplication(
      sharepoint_auth_config["client_id"], authority=sharepoint_auth_config["authority"],
      client_credential={"thumbprint": sharepoint_auth_config["thumbprint"], "private_key": open(sharepoint_auth_config['private_key_file']).read()}
    )
    
    result = app.acquire_token_for_client(scopes=sharepoint_auth_config["scope"])
    return result["access_token"]

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

  # Get Databricks API Bearer Token
  def get_databricks_bearer_token(scope, tenant_id, service_principal_id, service_principal_key, timeout=90):
    url = "https://login.microsoftonline.us/" + tenant_id + "/oauth2/token"
    
    data = CaseInsensitiveDict()
    data["grant_type"] = "client_credentials"
    data["client_id"] = service_principal_id
    data["client_secret"] = service_principal_key
    data["scope"] = scope
    data["resource"] ="2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
    
    response = requests.get(url, data=data, timeout=timeout)
    
    return json.loads(response.text)['access_token']

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # GET API Data
  def get_api(bearer_token, url, accept_type = "application/json",timeout=90):

    headers = CaseInsensitiveDict()
    headers["Accept"] = accept_type
    headers["Authorization"] = "Bearer " + bearer_token
    
    success = False
    retry_count = 0
    error_response = ""
    
    while not success and retry_count < 10:
      try:
        response = requests.get(url, headers=headers, timeout=timeout)
        success = True     
      except requests.ConnectionError as e:
        retry_count +=1
        error_response = str(e) + ' - Retry attempt #' + str(retry_count)
        print(error_response)
        time.sleep(10)
        
    if success:
      return response.json()  
    else:
      raise ValueError(error_response)

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # GET Databricks API Data
  def get_databricks_api(databricks_bearer_token, sp_bearer_token, databricks_resource_id, url, timeout=90):
    
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Bearer " + databricks_bearer_token
    headers["X-Databricks-Azure-SP-Management-Token"] = sp_bearer_token
    headers["X-Databricks-Azure-Workspace-Resource-Id"] = databricks_resource_id
        
    success = False
    retry_count = 0
    error_response = ""
    has_more = ""
    
    while not success and retry_count < 10:
      try:
        response = requests.get(url, headers=headers, timeout=timeout).json()
        has_more = response["has_more"]
        success = True     
      except requests.ConnectionError as e:
        retry_count +=1
        error_response = str(e) + ' - Retry attempt #' + str(retry_count)
        print(error_response)
        time.sleep(10)
        
    if success:
      return (response, has_more)
    else:
      raise ValueError(error_response)
      
  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # POST API Data
  def post_api(bearer_token, url, body = {}, timeout=90):
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"
    headers["Authorization"] = "Bearer " + bearer_token 
    
    success = False
    retry_count = 0
    error_response = ""
    
    data = json.dumps(body)
    
    while not success and retry_count < 10:
      try:
        response = requests.post(url, headers=headers, data=data, timeout=timeout)
        success = True     
      except requests.ConnectionError as e:
        retry_count +=1
        error_response = str(e) + ' - Retry attempt #' + str(retry_count)
        print(error_response)
        time.sleep(10)
        
    if success:
      if response is not None:
        response = response.json()
      else:
        response = {"No Response" : ""}
      
      return response
    
    else:
      raise ValueError(error_response)

    return response.json()

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Get From Type from Synapse
  def get_from_type(source_name,spark,jdbc_connection_string,jdbc_connection_Properties):  
    query = f""" (SELECT fromType FROM etl.fromSourceTypeLup WHERE fromSource = '{source_name}') as tmp"""
    df_from_type = spark.read.jdbc(url=jdbc_connection_string, table=query, properties=jdbc_connection_Properties)
    from_type = df_from_type.collect()[0][0]
    return from_type

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Get From Type from Synapse v2
  def get_from_type_v2(source_name,spark,jdbc_connection_string):  
    query = f""" (SELECT distinct fromType FROM etl.fromSourceTypeLup WHERE fromSource = '{source_name}')"""
    df_from_type = spark.read.format("com.databricks.spark.sqldw") \
                          .option("url", jdbc_connection_string + ";encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.usgovcloudapi.net;loginTimeout=30") \
                          .option("forwardSparkAzureStorageCredentials", "true") \
                          .option("enableServicePrincipalAuth", "true") \
                          .option("query",query)  
    df = df_from_type.load() 
    from_type = df.first()['fromType']
    return from_type  

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Custom Exception that will be raised if incorrect content type detected
  class IncorrectFileContentType(Exception):
    def __init__(self, message):
      base_message = "Incorrect Content Type Detected : "
      super().__init__(base_message + message)
      
  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Parent Validate Source File Content Type Function 
  def validate_source_file_content_types(blob_storage_connection_string, valid_content_type, file_ext, module_container_list, start_date_time, end_date_time):
    utc=pytz.UTC
    start_date_time = datetime.strptime(start_date_time, "%Y-%m-%dT%H:%M:%S" )
    end_date_time = datetime.strptime(end_date_time, "%Y-%m-%dT%H:%M:%S" )
    start_date_time = start_date_time.replace(tzinfo=utc)
    end_date_time = end_date_time.replace(tzinfo=utc)

    storage_metadata_dict = {}
    storage_metadata_dict = create_storage_account_metadata_dict(blob_storage_connection_string, module_container_list)
    
    for container_name in storage_metadata_dict:  
      for blob in storage_metadata_dict[container_name]:
        blob_name = blob.name
        blob_content_type = blob["content_settings"].content_type
        blob_last_modified_date = blob["last_modified"]
        blob_last_modified_date = blob_last_modified_date.replace(tzinfo=utc)
        if ((file_ext in blob_name) and
            (blob_content_type != valid_content_type) and 
            (blob_last_modified_date > start_date_time) and 
            (blob_last_modified_date < end_date_time)):     
          raise IncorrectFileContentType(f"Container - '{container_name}' || File - '{blob_name}' || Type - '{blob_content_type}'")
          
  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Returns Dictionary That Holds all Metadata for each Container Under that Module. Top Keys : Container Names --> Values : All Blob Metadata Relative to the Container. 
  def create_storage_account_metadata_dict(connection_string, module_container_list):
    storage_metadata_dict = {}
    storage_metadata_dict = create_nested_container_metadata_dict(connection_string, module_container_list)
    return storage_metadata_dict

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Create Blob Service Client from Connection String
  def get_service_client(connection_string):
    return BlobServiceClient.from_connection_string(connection_string)

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Returns Dictionary Containing metadata for each blob under each container
  def create_nested_container_metadata_dict(connection_string, container_names):
    container_blob_metadata_dict = {}
    for container in container_names:
      # create key of container name with value containing array of blob paths for that container
      container_blob_metadata_dict[container] = get_blob_metadata_list(connection_string, container)
    return container_blob_metadata_dict

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Returns List of all Blob Metadata (To be added to The Dictionary Created by 'create_nested_container_metadata_dict')
  def get_blob_metadata_list(connection_string, container_name):
    blob_service_client = get_service_client(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list_raw = container_client.list_blobs()
    blob_data_list = []
    for blob in blob_list_raw:
      blob_data_list.append(blob)         
    return blob_data_list

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Return Sas Token With Read/Write/List Permissions. Expires 4 hours after generated.
  def get_sas_token(connection_string, account_key):
    service_client = get_service_client(connection_string)
    return generate_account_sas(
      account_name = service_client.account_name,
      account_key = account_key, 
      resource_types = ResourceTypes(object=True, container=True),
      permission = AccountSasPermissions(read=True,write=True,list=True),
      expiry = datetime.utcnow() + timedelta(hours=4))

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Returns client for a specific blob. Used to upload a blob
  def get_blob_client(container_name, file_name, connection_string, account_key):
    service_client = get_service_client(connection_string)
    sas_token = get_sas_token(connection_string, account_key)
    return BlobClient(
          service_client.url,
          container_name = container_name, 
          blob_name = file_name,
          credential = sas_token)

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Delete Blob given connection string to storage account, container name, and blob_name
  def delete_blob(connection_string,container_name,blob_name):  

    container_client = ContainerClient.from_connection_string(conn_str=connection_string, container_name=container_name) 
    
    print("deleting " +container_name +"/"+ blob_name)
    container_client.delete_blob(blob=blob_name)
    
  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Copy blob from source to destination container. Uses recursion if blob copy was a failure, makes 10 attempts with 30 seconds between.
  def copy_blob(source_sas_token,source_connection_string,destination_connection_string,container_name, blob_name, copy_attempt=0):  
    # ================================ SOURCE ===============================

    # Create client
    source_blob_service_client = get_service_client(source_connection_string) 
    des_blob_service_client = get_service_client(destination_connection_string)    
    # Create blob client for source blob
    source_blob = BlobClient(
        source_blob_service_client.url,
        container_name = container_name, 
        blob_name = blob_name,
        credential = source_sas_token)
      
    # ============================= TARGET =======================================

    # Create target client
    target_client = des_blob_service_client.get_blob_client(container_name, blob_name)
    target_client.start_copy_from_url(source_blob.url)
    
    copy_status = target_client.get_blob_properties().copy["status"]
    
    begin_copy_datetime = datetime.now()
    copy_expired = False
    
    # Check Copy Status Every 10 Seconds
    while(copy_status == "pending" and not copy_expired):
      
      time.sleep(10)
      copy_status = target_client.get_blob_properties().copy["status"]

      if begin_copy_datetime + timedelta(minutes=10) <= datetime.now():
        copy_expired = True
        try:
          target_client.abort_copy(target_client.get_blob_properties().copy["id"])
          copy_status = "aborted"
        except ResourceExistsError:
          copy_status = "resourceExistsError"
        
      copy_status_details = target_client.get_blob_properties().copy["status_description"]
      
    # If Blob Copy Failed / Aborted AND Less Than 10 Attempts Have Been Made: Attempt To Copy Again After 30 Seconds
    
    # If file attempting to copy is currently being written to:
    if copy_status_details != None and "ConditionNotMet" in copy_status_details:
      results = ("ConditionNotMet",container_name, blob_name, f"attempts: {str(copy_attempt)}", copy_status_details)
      return results
    elif copy_status == "resourceExistsError":
      results = ("ResourceExistsError",container_name, blob_name, f"attempts: {str(copy_attempt)}", copy_status_details)
      return results
    elif copy_status != "success" and copy_attempt < 10:
      copy_attempt += 1
      time.sleep(30)
      results = copy_blob(source_sas_token,source_connection_string,destination_connection_string,container_name,blob_name, copy_attempt)
      return results
    else:
      results = (copy_status, container_name, blob_name, f"attempts: {str(copy_attempt)}", copy_status_details)
      return results
    
  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # ServiceNow Get Api. Error handling for refreshToken not existing in delta table / refresh token being expired
  def get_servicenow_api(servicenow_credentials,table_endpoint_parameter,spark,timeout=90):  
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = "Bearer " + servicenow_credentials["bearer_token"]
    
    success = False
    retry_count = 0
    error_response = ""
    url = servicenow_credentials["servicenow_url"]+f"/api/now/table/{table_endpoint_parameter}"
    
    while not success and retry_count < 10:
      try:
        time.sleep(1)
        response = requests.get(url, headers=headers, verify=False, timeout=timeout)
        success = True     
      #except requests.ConnectionError as e:
      except SocketError as err:   
        retry_count +=1
        error_response = str(err) + ' - Retry attempt #' + str(retry_count)
        print(error_response)
        time.sleep(10)

    if success:
      return response.json()
    else:
      raise ValueError(error_response)

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Post API to retrieve bearer token. 
  # If generate_new_token is set to True, a new refresh_token will be retrieved using the username and password and the new refresh token will be inserted into the delta table so future runs may re-use the token.
  # If generate_new_token is set to False, A lookup in the snRefreshTokenLup table will occur, and the refresh_token will be used to retrieve a new access_token (bearer_token).
  def get_servicenow_bearer_token(servicenow_credentials,spark,generate_new_token,error_message,timeout=90):
    
    servicenow_url = servicenow_credentials["servicenow_url"]
    servicenow_client_id = servicenow_credentials["servicenow_client_id"]
    servicenow_client_secret = servicenow_credentials["servicenow_client_secret"]
    servicenow_username = servicenow_credentials["servicenow_username"]
    servicenow_password = servicenow_credentials["servicenow_password"]
    
    url = f"{servicenow_url}/oauth_token.do"
    
    headers = CaseInsensitiveDict()    
    data = CaseInsensitiveDict()
    
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    data["client_id"] = servicenow_client_id
    data["client_secret"] = servicenow_client_secret
    
    if generate_new_token:   
      
      data["grant_type"] = "password"
      data["username"] = servicenow_username
      data["password"] = servicenow_password
      response = requests.post(url, headers=headers, data=data, verify=False, timeout=timeout)    
      bearer_token = json.loads(response.text)['access_token']
      refresh_token = json.loads(response.text)['refresh_token']
      if not (spark.sql(f"select distinct refreshToken from servicenow_rawdata.snRefreshTokenLup where refreshToken = '{refresh_token}'").count() > 0):
        time_generated = datetime.now()
        spark.sql(f"""insert into servicenow_rawdata.snRefreshTokenLup(
                  select '{refresh_token}' as refreshToken, cast('{time_generated}' as timestamp) as timeGenerated, '{error_message}' as tokenUpdateReason
                )""")
    else:
      
      df = spark.sql("select refreshToken from servicenow_rawdata.snRefreshTokenLup where timeGenerated = (select max(timeGenerated) from servicenow_rawdata.snRefreshTokenLup)")
      refresh_token = df.rdd.map(lambda x: str(x.refreshToken)).collect()[0]
      data["grant_type"] = "refresh_token"
      data["refresh_token"] = refresh_token
      response = requests.post(url, headers=headers, data=data, verify=False, timeout=timeout)    
      bearer_token = json.loads(response.text)['access_token']
      refresh_token = json.loads(response.text)['refresh_token']
    
    return bearer_token

  #---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
  # Get Library Version
  def get_cdefunctions_library_version():
    sprint = 33
    date_last_modified = "28 Oct 2022"
    print(f"Library Last Updated On Sprint {str(sprint)} ( {date_last_modified} )")
