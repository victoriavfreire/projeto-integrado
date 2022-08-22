# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Process
# MAGIC > Read all files in the client's DataLake
# MAGIC 
# MAGIC > Filter the new ones
# MAGIC 
# MAGIC > Write them to the bronze layer in Delta Lake format
# MAGIC 
# MAGIC > Save all the changes on the logs

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading previous data
# MAGIC ---
# MAGIC - The log file contains all the data already read and transformed
# MAGIC - Returns a list of all file names for filtering
# MAGIC - The writer function saves the read files on the log

# COMMAND ----------

def log_reader(PATH):  
    
    with open(PATH, 'r') as log:
        content = log.readline()
        read_files = content.split(',')
        
    return read_files


def log_writer(PATH, new_files, old_files=''):
    
    with open(PATH, 'w') as read_files:

        if not set(new_files).issubset(old_files):
            
            new_files = list(set(new_files))
            new_files.sort()
            all_files = set(new_files + old_files)
            
            concatened = ','.join(all_files)
            read_files.write(concatened)
        
            print(f'\nAdding {len(new_files)} entries to the log: ', *new_files, sep='\n\t--')
        
        else:
            print('\nNo new files found.')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading different formats of files and writing to a Delta Table
# MAGIC ---
# MAGIC - Reads JSON, CSV and PARQUET files
# MAGIC - Uses different configurations to read them
# MAGIC - Detects the separator in case of CSV format
# MAGIC ---
# MAGIC - Writes the data a Delta Lake directory
# MAGIC - Uses the file name to decide which directory to append the data
# MAGIC - Partitions the table by Federal State

# COMMAND ----------

def read_table(file, file_extension):
    from csv import Sniffer
    
    if file_extension == 'csv':
        
        sample_file = f"/{file.replace(':', '')}"
        with open(sample_file, 'r', encoding='ISO-8859-1') as data:
            csv_sample = data.readline()
        sniffer = Sniffer()
        dialect = sniffer.sniff(csv_sample)
        
        DELIMITER = dialect.delimiter
        INFER_SCHEMA = 'false'
        HEADER = 'true'
        
        df = (spark.read.format(file_extension)
              .option('encoding', 'ISO-8859-1')
              .option('inferSchema', INFER_SCHEMA)
              .option("header", HEADER)
              .option('sep', DELIMITER)
              .load(file)
             )
        
        return df
        
    
    elif file_extension == 'dbc':
        file = file.split('.')[0]
        
#         df = (spark.read
#                    .load(file, format='dbf'))
        print(f'.dbc format not supported, skipping the file {file}')

    else:
        df = (spark.read.format(file_extension)
              .option('encoding', 'ISO-8859-1')
              .load(file)
             )
        return df
      
        
def write_table(file_name, data_frame, PATH, write_mode):
    from pyspark.sql.functions import lit
    
    mode = {1: 'ambulatorial', 2: 'hospitalar'}
    file_type = mode[write_mode]
    
    if file_type == 'ambulatorial':
        if len(file_name) == 13:
            delta_table = file_name[:3]
            state_name = file_name[3:5]
            
            
        elif len(file_name) == 12:
            delta_table = file_name[:2]
            state_name = file_name[2:4]
        
        state_df = data_frame.withColumn('UF', lit(state_name))
        (state_df.write.format("delta")
             .option('mergeSchema', 'true')
             .partitionBy('UF')
             .mode("append")
             .save(f'{PATH}/{delta_table}'))
        
        print(f'| Wrote data to table {delta_table}, partition {state_name}.')
        return f'{PATH}/{delta_table}/UF={state_name}'
    
    elif file_type == 'hospitalar':
        delta_table = file_name[:3]
        state_name = file_name[4:6]
        year = file_name[7:11]
        state_df = data_frame.withColumn('UF', lit(state_name))
        
        (state_df.write.format("delta")
             .option('mergeSchema', 'true')
             .partitionBy('UF', 'ANO_CMPT')
             .mode("append")
             .save(f'{PATH}/{delta_table}'))
        
        print(f'| Wrote data to table {delta_table}, partition {state_name}/{year}.')
        return f'{PATH}/{delta_table}/UF={state_name}/ANO_CMPT={year}'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading all files in SUSBECANTT
# MAGIC ---
# MAGIC - Looping over every file
# MAGIC - Using the log data to choose the new ones
# MAGIC - Reading them with spark into a DataFrame
# MAGIC - Writing them to the Bronze Layer in Delta format

# COMMAND ----------

def travel_folders(LOG, FOLDERS):
    # Setting spark options
    SUPPORTED_EXTENSIONS = ['csv', 'json', 'parquet']
    PATH_BRONZE = "dbfs:/mnt/bronze/ingested_data"
    new_files = []
    updates_and_creations = []
    counter = 0
    
    # Looping over every file in the Container
    for write_mode, folder in FOLDERS.items():
        print(f'\nReading folder: {folder}...')
        for file in dbutils.fs.ls(folder):

            file_name = file[1]
            file_path = file[0]
            file_extension = file_name.split('.')[-1]

            if (file_name not in LOG) and (file_extension in SUPPORTED_EXTENSIONS):
                print(f'\tProcessing new file {file_name}', end=' ')
                try:
                    df = read_table(file_path, file_extension)
                    
                    update_or_creation = write_table(file_name, df, PATH_BRONZE, write_mode)

                except Exception as error:
                    print(f'An error ocurred: {error}')
                
                new_files.append(file_name)
                updates_and_creations.append(update_or_creation)
                
#                 counter += 1
#                 if counter >= 3:
#                     return new_files, updates_and_creations

    return new_files, updates_and_creations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main process
# MAGIC ---

# COMMAND ----------

def main():
    
    # Setting PATHs for log and Data folders
    LOG_PATH = '/dbfs/mnt/bronze/log/read_files.txt'
    UPDATE_PATH = '/dbfs/mnt/bronze/log/updated_or_created_files.txt'
    
    LOG = log_reader(LOG_PATH)
    UPDATE_LOG = log_reader(UPDATE_PATH)
    
    # 1: 'dbfs:/mnt/susbecantt/SIASUS/',
    # SIASUS folder not working
    RAW_FOLDERS = {2: 'dbfs:/mnt/susbecantt/SIHSUS/'}
    DATABASE = 'bronze'
    
    # Looping over every file and reading all tables
    new_files, updates_creations = travel_folders(LOG, RAW_FOLDERS)
    
    # Saving the new files to the log   
    log_writer(LOG_PATH, new_files, LOG)
    log_writer(UPDATE_PATH, updates_creations, UPDATE_LOG)
    
main()

# COMMAND ----------

