from pyspark.sql import SparkSession

def stdev_rand_list(size):
#  import statistics
#  from random import random
#  l = [(random()*(2**32)) for i in range(0,int(size+1*2))]
#  res = statistics.stdev(l)
  return size*size*size

spark = SparkSession.builder.getOrCreate()

#spark.sql('drop table if exists tuning_demo_output_table purge')

spark.udf.register("BUSY_FUNC", stdev_rand_list)
#spark.sql('''SELECT *, BUSY_FUNC(r) as r_func 
spark.sql('''SELECT * 
             FROM tuning_demo_input_table 
             ORDER BY key''')\
     .write.mode('overwrite')\
     .saveAsTable('tuning_demo_output_table')

#spark.udf.register("BUSY_FUNC_3X", stdev_rand_list_3x)
#final_df = spark.sql('SELECT key, BUSY_FUNC(50) as bs, BUSY_FUNC_3x(50) as bs3 FROM self_joined_table')
