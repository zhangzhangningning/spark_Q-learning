import pyspark 
from delta import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
import workload
import os.path
import sys
import store_res

builder = pyspark.sql.SparkSession.builder.appName("test") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.conf.set("spark.sql.files.maxRecordsPerFile", 60000)

deltaTable = DeltaTable.forPath(spark, "/tpch_delta_11/lineitem")


with open('/opt/share/CoWorkAlg/ColSelect.txt','r') as f:
    lines = f.readlines()
    last_line = lines[-1]

ColSelect = eval(last_line)

ZorderCol = []
for i in range(len(ColSelect)):
    if ColSelect[i] == 1:
        ZorderCol.append(workload.orgin_col[i])

if ZorderCol in store_res.Zordercol_res:
    pass
else:
    deltaTable.optimize().executeZOrderBy(ZorderCol)
    store_res.Zordercol_res.append(ZorderCol)

df = spark.read.format("delta").load("/tpch_delta_1/lineitem")
df.createOrReplaceTempView("lineitem")

df = spark.sql("select * from lineitem").withColumn("/tpch_delta_1/lineitem", input_file_name()).select(countDistinct("/tpch_delta_1/lineitem"))
total_files = df.collect()[0][0]

skip_files = 0
for i in range(len(workload.workload1)):
    key = str(ColSelect) + str(i)
    if key in store_res.skip_files_res:
        skip_file = store_res.skip_files_res[key]
    else:
        # 根据workload编码一次性执行多多次
        sql_sentence = workload.workload1[i]
        df = spark.sql(sql_sentence).withColumn("/tpch_delta_1/lineitem", input_file_name()).select(countDistinct("/tpch_delta_1/lineitem"))
        skip_file = total_files - df.collect()[0][0]
        store_res.skip_files_res[key] = skip_file
        skip_files += skip_file

with open('/opt/share/CoWorkAlg/skip_files.txt','a') as f:
    f.write(str(skip_files) + '\n')
   
