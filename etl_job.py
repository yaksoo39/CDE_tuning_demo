from pyspark.sql import SparkSession
import sys

def stdev_rand_list(size):
    return size*size*size

def main(username):
    spark = SparkSession.builder.getOrCreate()

    spark.udf.register("BUSY_FUNC", stdev_rand_list)

    input_table_name = f"tuning_demo_input_table_{username}"
    output_table_name = f"tuning_demo_output_table_{username}"

    spark.sql(f'''SELECT *, BUSY_FUNC(r) as r_func 
                  FROM {input_table_name} 
                  ORDER BY key''')\
         .write.mode('overwrite')\
         .saveAsTable(output_table_name)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark_script.py <username>")
        sys.exit(1)
    main(sys.argv[1])

