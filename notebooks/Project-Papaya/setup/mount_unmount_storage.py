# Databricks notebook source
#Connection properties
storage_account_name = "wtf1dl"
client_id = dbutils.secrets.get("formula1-secrets", "databricks-client-id")
tenant_id = dbutils.secrets.get("formula1-secrets", "databricks-tenant-id")
client_secret = dbutils.secrets.get("formula1-secrets", "databricks-service-secret")

# COMMAND ----------

#Setting storage related configs
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#Function to mount containers if they are not already mounted
def mount_adls(container_name):
    source = "abfss://"+container_name+"@"+storage_account_name+".dfs.core.windows.net/"
    mount_point = "/mnt/"+storage_account_name+"/"+container_name
    if any(mount_point in mount for mount in dbutils.fs.mounts()):
        print("Containter already mounted at", mount_point)
    else:
        dbutils.fs.mount(source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/", mount_point = f"/mnt/{storage_account_name}/{container_name}", extra_configs = configs)
        if any(mount_point in mount for mount in dbutils.fs.mounts()):
            print("Containter has been mounted: ", mount_point)
       

# COMMAND ----------

container_to_mount = ["raw", "processed"]
for container in container_to_mount:
    mount_adls(container)

# COMMAND ----------

#Function to unmount any mounted containers
def unmount_adls(container_name):
    source = "abfss://"+container_name+"@"+storage_account_name+".dfs.core.windows.net/"
    mount_point = "/mnt/"+storage_account_name+"/"+container_name
    if all(mount_point not in mount for mount in dbutils.fs.mounts()):
        print(container_name,"Containter already unmounted")
    else:
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
       

# COMMAND ----------

container_to_unmount = []
for container in container_to_unmount:
    unmount_adls(container)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

#mount new container for presentation layer
mount_adls("presentation")