# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Ingestion Process
# MAGIC ---

# COMMAND ----------

def update_partitions(updates_list, silver_path):
    for update in updates_list:
        print(f'Processing update {update}')
        
        table_name = update.split('/')[-3]
        
        if table_name == 'SIH':
            trans_df = transform_dataframe(update)
            
            write_to_silver(trans_df, silver_path, table_name)

# COMMAND ----------

def transform_dataframe(raw_df):
    import pyspark.sql.functions as F
    from pyspark.sql.types import IntegerType, FloatType
    
    df = spark.read.format('delta').load(raw_df)

    df = (df.withColumn('NASC', F.to_date(F.col('NASC'), 'yyyyMMdd'))
            .withColumn('DT_INTER', F.to_date(F.col('DT_INTER'), 'yyyyMMdd'))
            .withColumn('DT_SAIDA', F.to_date(F.col('DT_SAIDA'), 'yyyyMMdd'))
            .withColumn('IDADE', F.col('IDADE').cast(IntegerType()))
            .withColumn('UTI_MES_TO', F.col('UTI_MES_TO').cast(IntegerType()))
            .withColumn('DIAS_PERM', F.col('DIAS_PERM').cast(IntegerType()))
            .withColumn('QT_DIARIAS', F.col('QT_DIARIAS').cast(IntegerType()))
            .withColumn('VAL_SH', F.col('VAL_SH').cast(FloatType()))
            .withColumn('VAL_SP', F.col('VAL_SP').cast(FloatType()))
            .withColumn('VAL_TOT', F.col('VAL_TOT').cast(FloatType()))
            .withColumn('US_TOT', F.col('US_TOT').cast(FloatType()))
            .withColumn('COD_IDADE_CORR',
                         (F.when(F.col('IDADE').between(0, 10), 1)
                          .when(F.col('IDADE').between(11, 21), 2)
                          .when(F.col('IDADE').between(21, 35), 3)
                          .when(F.col('IDADE').between(36, 50), 4)
                          .when(F.col('IDADE').between(51, 70), 5)
                          .otherwise(6))
                        )
            .drop('_c0', 'UF_ZI', 'COD_IDADE')
         )

    return df

def write_to_silver(dataframe, silver_path, table_name):
    
    # Setting partition overwrite for updates
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    dataframe.write.mode("overwrite").insertInto(silver_path)
    print(f'\t\t| Wrote data to table {table_name}.')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading previous data
# MAGIC ---
# MAGIC - The log file contains all the data already read and waiting for transformation to silver
# MAGIC - Returns a list of all the pending partitions updates
# MAGIC - The writer function clears the log file from updates already completed

# COMMAND ----------

def log_reader(PATH):  
    
    with open(PATH, 'r') as log:
        content = log.readline()
        read_files = content.split(',')
        
    return read_files


def log_writer(PATH, new_files):
    
    with open(PATH, 'w') as read_files:

        if new_files:
            concatened = ','.join(new_files)
            read_files.write(concatened)

            print(f'\There are {len(new_files)} pending uptades to these patitions: ', *new_files, sep='\n\t--')
        
        else:
            read_files.write(' ')
            print('\nAll updates concluded.')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main process
# MAGIC ---

# COMMAND ----------

def main():
    
    # Setting PATH for log and reading pending changes
    GOLD_FLAG_PATH = '/dbfs/mnt/bronze/log/gold_ready.txt'
    UPDATE_PATH = '/dbfs/mnt/bronze/log/updated_or_created_files.txt'
    UPDATE_LOG = list(filter(None, log_reader(UPDATE_PATH)))
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    
    # Reading partitions and writing results to the silver layer
    SILVER_PATH = 'silver.sih'
    completed = update_partitions(UPDATE_LOG, SILVER_PATH)
    
    # Removing the completed updatese from the log
    if completed:
        
        pending_changes = list(set(UPDATE_LOG) - set(completed))
        log_writer(UPDATE_PATH, pending_changes)
        
        # Setting flag for Gold layer activation
        log_writer(GOLD_FLAG_PATH, ['Update gold layer'])
    else:
        print('Nothing to update on Gold layer')
    
main()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Bronze transformation
# MAGIC ---
# MAGIC - For manual use in case it's necessary to redo the whole silver layer
# MAGIC - Faster than looping through every partition

# COMMAND ----------

def full_SIH_transformation():
    import pyspark.sql.functions as F
    from pyspark.sql.types import IntegerType, FloatType
    
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    SILVER_PATH = 'dbfs:/mnt/silver/cleaned_data/SIH'
    RAW_DATA_PATH = 'dbfs:/mnt/bronze/ingested_data/SIH'
    
    df = spark.read.format('delta').load(RAW_DATA_PATH)
    
    df = (df.withColumn('NASC', F.to_date(F.col('NASC'), 'yyyyMMdd'))
            .withColumn('DT_INTER', F.to_date(F.col('DT_INTER'), 'yyyyMMdd'))
            .withColumn('DT_SAIDA', F.to_date(F.col('DT_SAIDA'), 'yyyyMMdd'))
            .withColumn('IDADE', F.col('IDADE').cast(IntegerType()))
            .withColumn('UTI_MES_TO', F.col('UTI_MES_TO').cast(IntegerType()))
            .withColumn('DIAS_PERM', F.col('DIAS_PERM').cast(IntegerType()))
            .withColumn('QT_DIARIAS', F.col('QT_DIARIAS').cast(IntegerType()))
            .withColumn('VAL_SH', F.col('VAL_SH').cast(FloatType()))
            .withColumn('VAL_SP', F.col('VAL_SP').cast(FloatType()))
            .withColumn('VAL_TOT', F.col('VAL_TOT').cast(FloatType()))
            .withColumn('US_TOT', F.col('US_TOT').cast(FloatType()))
            .withColumn('COD_IDADE_CORR',
                         (F.when(F.col('IDADE').between(0, 10), 1)
                          .when(F.col('IDADE').between(11, 21), 2)
                          .when(F.col('IDADE').between(21, 35), 3)
                          .when(F.col('IDADE').between(36, 50), 4)
                          .when(F.col('IDADE').between(51, 70), 5)
                          .otherwise(6))
                        )
            .drop('_c0', 'UF_ZI', 'COD_IDADE')
         )


    (df.write.format('delta')
        .mode('overwrite')
        .partitionBy('UF', 'ANO_CMPT')
        .saveAsTable('silver.sih'))
    
    print(f'Finished writing data to table SIH.')
    
    
# full_SIH_transformation()

# COMMAND ----------

