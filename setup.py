import setuptools

setuptools.setup(
    name = "cde_functions",
    version = "1.0",
    author = "Darpan Desai",
    author_email = "darpan.p.desai.dev@auds.army.mil",
    py_modules = ['cde_functions'],     
    python_requires = '>=3.6'
)

from .functions import *
from setuptools import setup

setup(name='cde_functions',
        version='1.0',
        description='Custom functions created for use within databricks.',        
        author=['Corey Lesko','Darpan Desai'],
        license='',
        packages=['cde_functions'],
        python_requires='>=3.6')


# Get From Type from Synapse
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
