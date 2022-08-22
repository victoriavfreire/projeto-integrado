# Databricks notebook source
def create_mount(sasURL,containerName,mountPointName,storageAccountName):

    sasKey = sasURL[sasURL.index('?'): len(sasURL)]
    dbutils.fs.mount(
      source = f"wasbs://{containerName}@{storageAccountName}.blob.core.windows.net/",
      mount_point = mountPointName,
      extra_configs = {f"fs.azure.sas.{containerName}.{storageAccountName}.blob.core.windows.net": sasKey}
    )


# COMMAND ----------

#mount gold
sasURL = "https://dungeonsdados.blob.core.windows.net/gold?sp=racwdlmeop&st=2022-08-19T17:51:54Z&se=2022-08-20T01:51:54Z&spr=https&sv=2021-06-08&sr=c&sig=EFOBEbBMbj7kfus06Hy94jk1j6o33hn4MAMcWLTog0A%3D"
storageAccount = "dungeonsdados"
containerName = "gold"
mountPoint = "/mnt/gold"
create_mount(sasURL,containerName,mountPoint,storageAccount)

# COMMAND ----------

#mount silver
sasURL = "https://dungeonsdados.blob.core.windows.net/silver?sp=racwdlmeop&st=2022-08-18T12:51:29Z&se=2022-09-18T20:51:29Z&spr=https&sv=2021-06-08&sr=c&sig=LOcf6wv40X3prABZulRspKkp5CDEuASCWnNLfmGI2c8%3D"
storageAccount = "dungeonsdados"
containerName = "silver"
mountPoint = "/mnt/silver"
create_mount(sasURL,containerName,mountPoint,storageAccount)

# COMMAND ----------

#mount bronze
sasURL = "https://dungeonsdados.blob.core.windows.net/bronze?sp=racwdlmeop&st=2022-08-10T19:31:25Z&se=2022-09-03T03:31:25Z&spr=https&sv=2021-06-08&sr=c&sig=OAZYLzzm%2FjfEceTK4I8u3RDPKYNaSdb5zscB4na8sdg%3D"
storageAccount = "dungeonsdados"
containerName = "bronze"
mountPoint = "/mnt/bronze"
create_mount(sasURL,containerName,mountPoint,storageAccount)

# COMMAND ----------

#mount susbecantt - external lake
sasURL = "https://susbecantt.blob.core.windows.net/susbecantt?sp=racwdli&st=2022-08-10T19:30:31Z&se=2022-09-10T03:30:31Z&spr=https&sv=2021-06-08&sr=c&sig=%2BjIveDUdQOwinYsGTKuIK2GVRV9ka0n6w2ZfC02hX0A%3D"
storageAccount = "susbecantt"
containerName = "susbecantt"
mountPoint = "/mnt/susbecantt"
create_mount(sasURL,containerName,mountPoint,storageAccount)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

