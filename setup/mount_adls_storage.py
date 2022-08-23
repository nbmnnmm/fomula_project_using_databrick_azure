# Databricks notebook source
client_id = 'db80b5dd-7ba7-4bc0-8ee8-6b79c44d8c75'
secret_id ='q948Q~jV8_MWEJIw-awpyDmLVCC.jf6yK4DLcb4m'
directory_id ='0aa16d8a-a396-4e21-aa14-2a68a45786bc'
storage_account_name ='databrickdl'

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id ,
           "fs.azure.account.oauth2.client.secret": secret_id,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name) : 
    dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/fomula/{container_name}",
  extra_configs = configs)

# COMMAND ----------

mount_adls('raw')

# COMMAND ----------

mount_adls('processed')

# COMMAND ----------

mount_adls('presentation')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/fomula/raw

# COMMAND ----------


