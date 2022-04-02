# Databricks notebook source
source = 'OSCE'
xml_veraion = '2.0'

# COMMAND ----------

poc_folder/xml

# COMMAND ----------

import json,os
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, LongType, TimestampType, BinaryType, IntegerType, DateType
from datetime import datetime
import re
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %fs ls /mnt/trendmicrobif/xmlsource

# COMMAND ----------

root = '/dbfs/mnt/trendmicrobif/xmlsource/'
f_list = [] 
f_dict = {}
for fname in os.listdir(root):
  f_dict['file_name'] = fname
  f_dict['file_path'] = root + fname
  f_list.append(f_dict)

# COMMAND ----------

def read_xml_file(path):
  with open(path, 'r' ) as f:
    xml_str = f.read()
  return xml_str

read_xml_file_udf = udf(read_xml_file, StringType())

file_info_schema = StructType([
    StructField('file_date', DateType(), True),
    StructField('xml_version', StringType(), True),
    StructField('ip', StringType(), True)
  
])

def file_name_split(path):
  file_info = {}
  list = path.split('_')
  file_info['file_date'] =  datetime.strptime(list[1], "%Y%m%d%H%M%S%f")
  file_info['xml_version'] = list[2]
  file_info['ip'] = list[3]
  return file_info

file_name_split_udf = udf(file_name_split, file_info_schema)

  

# COMMAND ----------

file_rdd = spark.sparkContext.parallelize(f_list)
file_df = (spark.createDataFrame(file_rdd)
          .select('file_name', read_xml_file_udf('file_path').alias('file_content'))
          .withColumn('file_info',file_name_split_udf('file_name'))
          .select('file_info.*','file_name','file_content')
          )

display(file_df)

# COMMAND ----------

# get raw data
with open("/dbfs//mnt/trendmicrobif/TBL_DATA_RAW.json", "r") as f:
    raw_str = f.read()
raw_json = json.loads(raw_str)
raw_array = raw_json[0]['data']
raw_schema = raw_json[0]['schema']

def string_to_date(x): 
  return datetime.strptime(x.split('.')[0], "%Y-%m-%dT%H:%M:%S")

string_to_date_udf = udf(string_to_date, DateType())

# Create StructType
raw_schema = StructType([
    StructField('file_id', StringType(), True),
    StructField('source', StringType(), True),
    StructField('xml_version', StringType(), True),
    StructField('file_name', StringType(), True),
  StructField('file_content', StringType(), True),
  StructField('file_date', StringType(), True),
  StructField('ip', StringType(), True)
])
rdd = spark.sparkContext.parallelize(raw_array)

# Create DataFrame
df_raw = (spark.createDataFrame(rdd,raw_schema)
          .withColumn('file_date', string_to_date_udf(F.col('file_date')))
          .write.format('delta')
          .mode('overwrite')
          .save('/mnt/trendmicrobif/TBL_DATA_RAW')
         )
display(df_raw)

# COMMAND ----------

df_raw = (spark.read.format('delta')
          .load('/mnt/trendmicrobif/TBL_DATA_RAW')
          .filter(F.col('source') == source)
          .filter(F.col('xml_version') == xml_veraion )
     )
df_raw.createOrReplaceTempView('TBL_DATA_RAW')
display(df_raw)

# COMMAND ----------

df_xpath = (spark.read.format('csv')
         .option('header', True)
         .load('/mnt/trendmicrobif/TBL_RAW_XPATH.csv')
         .filter(F.col('source') == source)
         .filter(F.col('xml_version') == xml_veraion)
         
        )
df_config = (spark.read.format('csv')
         .option('header', True)
         .load('/mnt/trendmicrobif/TBL_XML_RAW_CONFIG.csv')
         .filter(F.col('source') == source)
         .filter(F.col('xml_version') == xml_veraion)
         
        )

# COMMAND ----------

#get config
df_xpath_config = (df_xpath.alias('r').join(df_config.alias('c'), [F.col('r.source')==F.col('c.source'),
                                                                F.col('r.xml_version')==F.col('c.xml_version'),
                                                                F.col('r.xpath')==F.col('c.xpath')
                                                               ], 'left')
                  .withColumn('level', F.coalesce('c.level',F.lit('0')))
                  .withColumn('raw_group_root', F.coalesce('c.raw_group_root','r.xpath'))
                  .withColumn('group_root', F.coalesce('c.group_root','r.xpath'))
                  .withColumn('master_detail', F.coalesce('c.master_detail',F.lit('M')))
                  .withColumn('column_name', F.coalesce('c.default_column_name','r.xpath'))
                  .withColumn('data_type', F.coalesce('c.default_data_type',F.lit('varchar')))
                  .withColumn('data_length', F.coalesce('c.default_data_length',F.lit('1000')))
                  .withColumn('sample_value', F.coalesce('c.sample_value',F.lit('')))
                  .withColumn('ID', F.row_number().over(Window.orderBy(F.col('r.source'),F.col('r.xml_version'),F.col('r.xpath'))))
                  .select('ID', 'r.source', 'r.xml_version', 'level', 'r.xpath', 'raw_group_root', 'group_root', 'master_detail', 'column_name', 'data_type', 'data_length', 'sample_value')
                  )
display(df_xpath_config)  

# COMMAND ----------

data = [('test_data','<data><product_info><hardware_info><cpu>Intel(R) Xeon(R) Gold 5220 CPU @ 2.20GHz x 1 core(s)</cpu><cpu>Intel(R) Xeon(R) Gold 5220 CPU @ 2.20GHz x 1 core(s)</cpu><hdd_partitions><filestores><avail>175981248</avail><size>197460232</size></filestores><varlog><avail>64712540</avail><size>103212320</size></varlog></hdd_partitions><hdd_size>314572800</hdd_size></hardware_info></product_info></data>')]
df = spark.createDataFrame(data,["file_name","file_content"])
df.createOrReplaceTempView('testdata')


# COMMAND ----------

# MAGIC %sql 
# MAGIC select file_name,xpath(file_content, '/data/product_info/hardware_info/cpu/text()') from testdata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select b.file_id, xpath(b.file_content,'/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/ByCount/text()') as value from TBL_DATA_RAW b
# MAGIC select b.file_id, xpath(b.file_content,'/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/@ByCount') as value from TBL_DATA_RAW b

# COMMAND ----------

def list_to_lenght(list): 
  return len(list)
list_to_lenght_udf = udf(list_to_lenght, IntegerType())

# COMMAND ----------

# update master_detail, raw_group_root, group_root, level, column_name
update_set = []
for row in df_xpath_config.collect():
    xpath = row.xpath #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/@ByCount
    xpath_list = xpath.split('/')
    xpath_full_text = '/'.join(xpath_list[:-1])+'/'+xpath_list[-1].replace('@','') +'/text()' #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/ByCount/text()
    xpath_full = '/'.join(xpath_list[:-1])+'/@'+xpath_list[-1].replace('@','')  #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/@ByCount
    group_root = '/'.join(xpath_list[:-2])+'/'+xpath_list[-2] #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item
   
    
    update_dict = {}
    if(row.level == '0'):
      master_detail = 'M'
      sql_script = f"select file_id, xpath(file_content,'{xpath_full}') as value from TBL_DATA_RAW"
      xpath_tmp = (spark
                   .sql(sql_script)
                   .select('file_id', list_to_lenght_udf('value').alias('count'))
                   .filter(F.col('count')>1)
                   )
      if(xpath_tmp.count() > 0): 
        master_detail = 'D'
        update_dict['ID'] = row.ID
        update_dict['xpath'] = xpath
        update_dict['raw_group_root'] = group_root
        update_dict['master_detail'] = master_detail
        update_set.append(update_dict)
      else:
        sql_script = f"select file_id, xpath(file_content,'{xpath_full_text}') as value from TBL_DATA_RAW"
        xpath_tmp = (spark
                     .sql(sql_script)
                     .select('file_id', list_to_lenght_udf('value').alias('count'))
                     .filter(F.col('count')>1)
                     )
        if(xpath_tmp.count() > 0): 
          master_detail = 'D'
          update_dict['ID'] = row.ID
          update_dict['xpath'] = xpath
          update_dict['raw_group_root'] = xpath
          update_dict['master_detail'] = master_detail

          update_set.append(update_dict)



# COMMAND ----------

def xpath_to_level(xpath): 
  return len(xpath.split('/'))-1

xpath_to_level_udf = udf(xpath_to_level, IntegerType())

def xpath_to_columnname(xpath): 
  xpath_list = xpath.split('/')
  return xpath_list[-1].replace('@','')

xpath_to_columnname_udf = udf(xpath_to_columnname, StringType())


update_rdd= spark.sparkContext.parallelize(update_set)
update_df = (spark
             .createDataFrame(update_rdd)
             .withColumn('level',xpath_to_level_udf('xpath'))
             .withColumn('column_name',xpath_to_columnname_udf('xpath'))
            )


display(update_df)

# COMMAND ----------

df_xpath_config_1 = (df_xpath_config.alias('con').join(update_df.alias('update'), F.col('update.ID')==F.col('con.ID'),'left')
             .withColumn('level1',F.when(F.col('update.level').isNull(), F.col('con.level')).otherwise(F.col('update.level')))
             .withColumn('master_detail1', F.when(F.col('update.master_detail').isNull(), F.col('con.master_detail')).otherwise(F.col('update.master_detail')))
             .withColumn('column_name1',F.when(F.col('update.column_name').isNull(), F.col('con.column_name')).otherwise(F.col('update.column_name')))
             .withColumn('raw_group_root1', F.when(F.col('update.raw_group_root').isNull(), F.col('con.raw_group_root')).otherwise(F.col('update.raw_group_root')))
             .withColumn('group_root1', F.col('raw_group_root1'))
             .select('con.ID', 'con.source', 'con.xml_version', 'level1', 'con.xpath', 'raw_group_root1', 'group_root1', 'master_detail1', 'column_name1', 'con.data_type', 'con.data_length', 'con.sample_value')
              .withColumnRenamed('master_detail1', 'master_detail')
              .withColumnRenamed('raw_group_root1', 'raw_group_root')
              .withColumnRenamed('level1', 'level')
              .withColumnRenamed('group_root1', 'group_root')
              .withColumnRenamed('column_name1', 'column_name')
  )

display(df_xpath_config_1)
#         .filter(F.col('ID')=='399'))

# COMMAND ----------

display(df_xpath_config.filter(F.col('ID')=='299'))
display(df_xpath_config_1.filter(F.col('ID')=='299'))

# COMMAND ----------

# update sample_data , data_type ,data_length

update_set = []
for row in df_xpath_config_1.collect():
    xpath = row.xpath #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/@ByCount
    xpath_list = xpath.split('/')
    xpath_full_text = '/'.join(xpath_list[:-1])+'/'+xpath_list[-1].replace('@','') +'/text()' #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/ByCount/text()
    xpath_full = '/'.join(xpath_list[:-1])+'/@'+xpath_list[-1].replace('@','')  #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/@ByCount
    group_root = '/'.join(xpath_list[:-2])+'/'+xpath_list[-2] #/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item

    update_dict = {}
    if(not row.sample_value):
    
      sql_script = f"select file_id, xpath(file_content,'{xpath_full}') as value from TBL_DATA_RAW"
      xpath_tmp = (spark
                   .sql(sql_script)
                   .select('file_id', 'value',list_to_lenght_udf('value').alias('count'))
                   .filter(F.col('count')>1)
                   .select('file_id', F.explode('value').alias('value'))
                   )
      

      if(xpath_tmp.count() > 0): 
        update_dict['ID'] = row.ID
        update_dict['xpath'] = xpath
        update_dict['sample_value'] = xpath_tmp.rdd.collect()[0][1] 
        update_set.append(update_dict)
    else:
      sql_script = f"select file_id, xpath(file_content,'{xpath_full_text}') as value from TBL_DATA_RAW"
      xpath_tmp = (spark
                   .sql(sql_script)
                   .select('file_id', 'value',list_to_lenght_udf('value').alias('count'))
                   .filter(F.col('count')>1)
                   .select('file_id', F.explode('value').alias('value'))
                   )
      if(xpath_tmp.count() > 0): 
        master_detail = 'D'
        update_dict['ID'] = row.ID
        update_dict['xpath'] = xpath
        update_dict['sample_value'] = xpath_tmp.rdd.collect()[0][1] 
        update_set.append(update_dict)
        

# COMMAND ----------

def is_number(num):
  pattern = re.compile(r'^[-+]?[-0-9]\d*\.\d*|[-+]?\.?[0-9]\d*$')
  result = pattern.match(num)
  if result:
    return True
  else:
    return False
  
def is_valid_data(str):
  try:
    if ":" in str:
      time.strptimr(str,"%Y-%m-%d %H:%M%:S")
    else:
      time.strptimr(str,"%Y-%m-%d")
    return True
  except:
    return False
  
data_schema = StructType([
    StructField('data_type', StringType(), True),
    StructField('data_length', StringType(), True)
])

def setDatatype(value):
  data_type = ''
  data_length = ''
  data_info = {}
  if(len(value)>8 and is_valid_data(value)):
      data_type = "datetime"
      data_length = ""
  elif(is_number(value)):
    
    whole_part =  len(str(value).split(".")[0])
    decimal_part = 0
    if (len(str(value).split(".")) == 2) :     
      decimal_part = len(str(value).split(".")[1])
      
    if(decimal_part == 0 ):
      data_type = "int"
      data_length = ""
    else:
      datatype = "numeric"
      data_length = f"{str(whole_part+decimal_part)},{decimal_part}"
  else:
    data_type = "varchar"    
    data_length = len(value)
    
  data_info['data_type'] = data_type
  data_info['data_length'] = data_length                     
  return data_info
                     
setDatatype_udf = udf(setDatatype,data_schema)

# COMMAND ----------

update_rdd= spark.sparkContext.parallelize(update_set)
update_df = (spark
             .createDataFrame(update_rdd)
             .withColumn('data_info', setDatatype_udf('sample_value'))
             .select('ID','xpath','sample_value','data_info.*')
            )


display(update_df)

# COMMAND ----------

df_xpath_config_2 = (df_xpath_config_1.alias('con').join(update_df.alias('update'), F.col('update.ID')==F.col('con.ID'),'left')
             .withColumn('sample_value1',F.when(F.col('update.sample_value').isNull(), F.col('con.sample_value')).otherwise(F.col('update.sample_value')))
             .withColumn('data_type1', F.when(F.col('update.data_type').isNull(), F.col('con.data_type')).otherwise(F.col('update.data_type')))
             .withColumn('data_length1',F.when(F.col('update.data_length').isNull(), F.col('con.data_length')).otherwise(F.col('update.data_length')))
             .select('con.ID', 'con.source', 'con.xml_version', 'con.level', 'con.xpath', 'con.raw_group_root', 'con.group_root', 'con.master_detail', 'con.column_name', 'data_type1', 'data_length1', 'sample_value1')
              .withColumnRenamed('data_type1', 'data_type')
              .withColumnRenamed('data_length1', 'data_length')
              .withColumnRenamed('sample_value1', 'sample_value')

  )

display(df_xpath_config_2)
#         .filter(F.col('ID')=='299'))

# COMMAND ----------

display(df_xpath_config.filter(F.col('ID')=='299'))
display(df_xpath_config_1.filter(F.col('ID')=='299'))
display(df_xpath_config_2.filter(F.col('ID')=='299'))

# COMMAND ----------

display(df_xpath_config_2)
df_xpath_config_2.write.saveAsTable('df_xpath_config_2', mode='overwrite')

# COMMAND ----------

# sql_script = "select b.file_id, xpath(b.file_content,'/Data/Products/Product/Server/LogMaintenance/ScheduleDelete/ScheduledDeleteItems/Item/@ByCount') as value from TBL_DATA_RAW b"
# xpath_tmp = (spark
#                .sql(sql_script)
#                .select('file_id', 'value',list_to_lenght_udf('value').alias('count'))
#                .filter(F.col('count')>1)
#                .select('file_id', F.explode('value').alias('sample_value'))
#                .withColumn('data_info', setDatatype_udf('sample_value'))
#                .select('file_id','sample_value','data_info.*')
#              )

# display(xpath_tmp)
