from random import random
from pyspark.sql import SparkSession

scale = 2000
spark = SparkSession.builder.getOrCreate()

for key in range(0,6):
    spark.sparkContext.parallelize(range(0,scale))\
                      .map(lambda x: (key,int(random()*(2**32-1))))\
                      .toDF(['key','value'])\
                      .createOrReplaceTempView('key' + str(key) + '_table')

spark.sql('''SELECT a.key, a.value 
             FROM key0_table a 
             JOIN (SELECT * FROM key0_table LIMIT 5) b 
             ON a.key = b.key''').createOrReplaceTempView('key0_5x_table') 

spark.sql('''SELECT * FROM key1_table 
             UNION ALL 
             SELECT * FROM key2_table 
             UNION ALL 
             SELECT * FROM key3_table 
             UNION ALL 
             SELECT * FROM key4_table 
             UNION ALL 
             SELECT * FROM key5_table 
             UNION ALL 
             SELECT * FROM key0_5x_table''').createOrReplaceTempView('skewed_table')

df = spark.sql('''SELECT a.key, rand() as r 
                  FROM skewed_table a 
                  JOIN skewed_table b 
                  ON a.key = b.key ORDER BY r''')

df.coalesce(4).write.mode('overwrite').saveAsTable('tuning_demo_input_table')

spark.stop()
