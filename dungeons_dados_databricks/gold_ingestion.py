# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Ingestion Process
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cancer Fact Table

# COMMAND ----------

from pyspark.sql.functions import desc, concat
df = spark.read.format('delta').load('dbfs:/mnt/silver/cleaned_data/SIH')

# COMMAND ----------

# grouped = df.groupBy('NASC', 'SEXO', 'CEP', 'DIAG_PRINC').concat().alias("ID")

display(df.select(concat(df.DIAG_PRINC, df.NASC, df.SEXO, df.CEP).alias("ID")))

# display(df.filter((df.NASC  == '2002-11-18') & (df.SEXO  == 3) & (df.CEP  == "57570000") & (df.DIAG_PRINC == 'C761')))

# display(grouped.filter(grouped.DIAG_PRINC.contains('C76')))

# COMMAND ----------

# display(df.filter(df.DIAG_PRINC.contains('C76')).groupBy('DIAG_PRINC').mean('QT_DIARIAS'))

# display(df.filter(df.DIAG_PRINC.contains('C76')))

# display(df.filter('UF == CE'))

print((df.count(), len(df.columns)))

print(df.rdd.getNumPartitions())

# COMMAND ----------

from pyspark.sql import functions as F

grouped_cancer = df.groupBy('NASC', 'SEXO', 'CEP', 'DIAG_PRINC').agg(F.sum('QT_DIARIAS'), F.count('NASC'))
display(grouped_cancer.filter(df.DIAG_PRINC.contains('C76')))

# COMMAND ----------


