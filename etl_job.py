from pyspark.sql import SparkSession

def stdev_rand_list(size):
  return size*size*size

spark = SparkSession.builder.getOrCreate()

spark.udf.register("BUSY_FUNC", stdev_rand_list)

spark.sql('''SELECT *, BUSY_FUNC(r) as r_func 
             FROM tuning_demo_input_table 
             ORDER BY key''')\
     .write.mode('overwrite')\
     .saveAsTable('tuning_demo_output_table')
