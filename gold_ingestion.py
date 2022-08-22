# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Ingestion Process
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading update flag
# MAGIC ---
# MAGIC - The log file contains the flag that signals an update on the gold layer
# MAGIC - Returns **True** if there was an update on silver, **False** otherwise

# COMMAND ----------

def log_reader(PATH):  
    
    with open(PATH, 'r') as log:
        content = log.readline()
        
        if content == 'Update gold layer':
            return True
        
        else:
            return False
        
        
def log_writer(PATH):
    
    with open(PATH, 'w') as read_files:
            read_files.write(' ')
            print('\nGold layer update concluded.')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cancer Fact Table
# MAGIC - Patient ID
# MAGIC - Admission date
# MAGIC - CENS
# MAGIC - Mean hospitalization time for each type of cancer

# COMMAND ----------

def create_cancer_table(dataframe):  
    dataframe = dataframe.withColumn('ID_PAC', F.concat(dataframe.DIAG_PRINC, dataframe.NASC, dataframe.SEXO, dataframe.CEP))
    
    cid_part = Window.partitionBy('DIAG_PRINC')
    uf_cid_part = Window.partitionBy('UF', 'DIAG_PRINC')
    cid_sex_part = Window.partitionBy('DIAG_PRINC', 'SEXO')
    cid_codidade = Window.partitionBy('DIAG_PRINC', 'COD_IDADE_CORR')
    
    fact_df = (dataframe.select('ID_PAC', 
                             'DT_INTER',
                             'UF',
                             'ANO_CMPT',
                             'CNES',
                             'DIAG_PRINC',
                             F.round(F.mean('QT_DIARIAS').over(cid_part), 2).alias('MED_DIA_POR_CID'),
                             F.count('DIAG_PRINC').over(cid_sex_part).alias('TOT_SEXO_CID'),
                             F.count('COD_IDADE_CORR').over(cid_codidade).alias('FX_ET_POR_CID'),
                             F.round(
                                     F.count('DIAG_PRINC').over(uf_cid_part), 3)
                              .alias('TOT_CID_UF')
                            )
              )
    
    return fact_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Patient Dimension Table
# MAGIC > **Patient ID**
# MAGIC - Date of birth
# MAGIC - ZIP code
# MAGIC - Death
# MAGIC - Race
# MAGIC - Age
# MAGIC - Age category
# MAGIC - AIH
# MAGIC - City of residency
# MAGIC - CID10
# MAGIC - State

# COMMAND ----------

def create_patient_dim(dataframe):
    
    patientD = (dataframe.withColumn('ID_PAC', F.concat(dataframe.DIAG_PRINC, dataframe.NASC, dataframe.SEXO, dataframe.CEP))
                         .withColumn('COD_IDADE_LABEL',
                                     (F.when(F.col('COD_IDADE_CORR') == 1, '0 a 10 anos')
                                      .when(F.col('COD_IDADE_CORR') == 2, '11 a 20 anos')
                                      .when(F.col('COD_IDADE_CORR') == 3, '21 a 35 anos')
                                      .when(F.col('COD_IDADE_CORR') == 4, '36 a 50 anos')
                                      .when(F.col('COD_IDADE_CORR') == 5, '51 a 70 anos')
                                      .otherwise('70+ anos'))
                                    )      
                         .withColumn('MORTE_LABEL',
                                     (F.when(F.col('MORTE') == 0, 'Sem óbito')
                                      .when(F.col('MORTE') == 1, 'Com óbito'))
                                    ) 
                         .withColumn('SEXO_LABEL',
                                     (F.when(F.col('SEXO') == 1, 'Masculino')
                                      .when(F.col('SEXO') == 2, 'Feminino')
                                      .when(F.col('SEXO') == 3, 'Ignorado'))
                                    )  
                         .withColumn('RACA_COR_LABEL',
                                     (F.when(F.col('RACA_COR') == 1, 'Branca')
                                      .when(F.col('RACA_COR') == 2, 'Preta')
                                      .when(F.col('RACA_COR') == 3, 'Parda')
                                      .when(F.col('RACA_COR') == 4, 'Amarela')
                                      .when(F.col('RACA_COR') == 5, 'Indígena')
                                      .when(F.col('RACA_COR') == 99, 'Sem informação'))
                                    )  
                         .dropDuplicates(subset = ['ID_PAC'])
                         .select('ID_PAC', 
                                 'NASC', 
                                 'CEP',
                                 'SEXO_LABEL',
                                 'RACA_COR_LABEL',
                                 'IDADE',
                                 'COD_IDADE_LABEL',
                                 'MORTE_LABEL',
                                 'N_AIH',
                                 'MUNIC_RES',
                                 'DIAG_PRINC',
                                 'UF',
                                 'ANO_CMPT'
                                 ))
    
    return patientD

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hospital Dimension Table
# MAGIC > **CNES**
# MAGIC - Hospital city
# MAGIC - Hospital state
# MAGIC - Days of ICU
# MAGIC - Days of hospitalization
# MAGIC - AIH
# MAGIC - Hospitalization date
# MAGIC - Discharge date

# COMMAND ----------

def create_hospital_dim(dataframe):
    part = Window.partitionBy('CNES', 'MES_CMPT')
    
    hospitalD = (dataframe.select('CNES',
                                  'MUNIC_MOV',
                                  'ANO_CMPT',
                                  'MES_CMPT',
                                  'UF',
                                  F.sum('QT_DIARIAS').over(part).alias('TOT_DIARIAS_HOSP'),
                                  F.sum('UTI_MES_TO').over(part).alias('TOT_UTI_HOSP')
                                 ).dropDuplicates()
                )
    
    return hospitalD

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Dimension Table
# MAGIC > **Date of birth**
# MAGIC 
# MAGIC > **Date of hospitalization**
# MAGIC 
# MAGIC - Year
# MAGIC - Month

# COMMAND ----------

def create_date_dim(dataframe):
    
    dataframe = (dataframe.select('NASC',
                                  'DT_INTER',
                                  'MES_CMPT',
                                  'ANO_CMPT',
                                  'UF'
                                  )
                          .dropDuplicates()
                )
    
    return dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### Localization Dimension Table
# MAGIC - State
# MAGIC - Hospital City
# MAGIC - Pacient City

# COMMAND ----------

def create_local_dim(dataframe):
    
    localD = (dataframe.withColumn('MUNIC_DIFF', (F.col('MUNIC_MOV') != F.col('MUNIC_RES')))
                          .select('MUNIC_DIFF',
                                  'UF',
                                  'ANO_CMPT',
                                  'MUNIC_MOV',
                                  'MUNIC_RES',
                                  )
                          .dropDuplicates()
                )
    
    return localD

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Process
# MAGIC ---

# COMMAND ----------

def main():
    import pyspark.sql.functions as F
    from pyspark.sql import Window
    
    print('main function ran')
    df = spark.read.table('silver.sih')
    df = df.filter(df.DIAG_PRINC.contains('C76'))
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    fact_df = create_cancer_table(df)
    (fact_df.write
             .mode("overwrite")
             .partitionBy('UF', 'ANO_CMPT')
             .saveAsTable("gold.CANCER_FACT"))
    print('Finished Cancer fact table.')
    
    patientD = create_patient_dim(df)
    (patientD.write
            .mode("overwrite")
            .partitionBy('UF', 'ANO_CMPT')
            .saveAsTable("gold.PATIENT_D"))
    print('Finished Patient dimension table.')
    
    dateD = create_date_dim(df)
    (dateD.write
            .mode("overwrite")
            .partitionBy('UF', 'ANO_CMPT')
            .saveAsTable("gold.DATE_D"))
    print('Finished Date dimension table.')
    hospitalD = create_hospital_dim(df)
    (hospitalD.write
             .mode("overwrite")
             .partitionBy('UF')
             .saveAsTable("gold.HOSPITAL_D"))
    print('Finished Hospital dimension table.')
    localD = create_local_dim(df)
    (localD.write
            .mode("overwrite")
            .partitionBy('UF')
            .saveAsTable("gold.LOCAL_D"))
    print('Finished Local dimension table.')

GOLD_FLAG_PATH = '/dbfs/mnt/bronze/log/gold_ready.txt'
UPDATE_FLAG = log_reader(GOLD_FLAG_PATH)

if UPDATE_FLAG:
    main()
    log_writer(GOLD_FLAG_PATH)
    
else:
    print('Gold tables are up to date!')

# COMMAND ----------

