# Databricks notebook source
# MAGIC %md
# MAGIC #### Main process
# MAGIC ---

# COMMAND ----------

def main():
    
    # Setting PATH for log and reading pending changes
    UPDATE_PATH = '/dbfs/mnt/bronze/log/updated_or_created_files.txt'
    UPDATE_LOG = list(filter(None, log_reader(UPDATE_PATH)))
    
    # Reading partitions and writing results to the silver layer
    SILVER_PATH = 'dbfs:/mnt/silver/cleaned_data'
    completed = update_partitions(UPDATE_LOG, SILVER_PATH)
    
    # Removing the completed updatese from the log
#     pending_changes = list(set(UPDATE_LOG) - set(completed))
#     log_writer(UPDATE_PATH, pending_changes)
    
main()

# COMMAND ----------

def update_partitions(updates_list, silver_path):
    completed_updates = []
    for update in updates_list:
        print(f'Processing update {update}')
        
        table_name = update.split('/')[-3]
        
        if table_name == 'SIH':
            trans_df = transform_dataframe(update)
            
            write_to_silver(trans_df, silver_path, table_name)
            completed_updates.append(update)
            
    return completed_updates
            

# COMMAND ----------

def transform_dataframe(raw_df):
    from pyspark.sql.functions import desc, col, to_date
    from pyspark.sql.types import IntegerType, FloatType
    
    df = spark.read.format('delta').load(raw_df)

    df = (df.withColumn('NASC', to_date(col('NASC'), 'yyyyMMdd'))
            .withColumn('DT_INTER', to_date(col('DT_INTER'), 'yyyyMMdd'))
            .withColumn('DT_SAIDA', to_date(col('DT_SAIDA'), 'yyyyMMdd'))
            .withColumn('IDADE', col('IDADE').cast(IntegerType()))
            .withColumn('DIAS_PERM', col('DIAS_PERM').cast(IntegerType()))
            .withColumn('QT_DIARIAS', col('QT_DIARIAS').cast(IntegerType()))
            .withColumn('VAL_SH', col('VAL_SH').cast(FloatType()))
            .withColumn('VAL_SP', col('VAL_SP').cast(FloatType()))
            .withColumn('VAL_TOT', col('VAL_TOT').cast(FloatType()))
            .withColumn('US_TOT', col('US_TOT').cast(FloatType()))
            .drop('_c0', 'UF_ZI')
         )

    return df

def write_to_silver(dataframe, silver_path, table_name):
    (dataframe.write.format('delta')
        .mode('overwrite')
#         .option('replaceWhere', "UF == 'AC' AND ANO_CMPT == '2008'")
        .save(f'{silver_path}/{table_name}'))

#     print(f'{silver_path}/{table_name}')
#     (dataframe.write
#          .option('path', f'{silver_path}/{table_name}/UF=AC/')
#          .saveAsTable(name = 'ANO_CMPT=2008',
#                       format = 'delta',
#                       partitonBy = ['UF', 'ANO_CMPT'],
#                       mode = 'overwrite'))
    
    print(f'\t\t| Wrote data to table {table_name}.')

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

string = 'dbfs:/mnt/bronze/ingested_data/SIH/UF=AC/ANO_CMPT=2008'

string.split('/')[-3:]

# COMMAND ----------


