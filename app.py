import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

# spark = SparkSession.builder.appName("MySQL connection").getOrCreate()
# # place the jar file in 
# df = jdbcDF = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:mysql://localhost:3306/adventureworks") \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("dbtable", "employee") \
#     .option("user", "root") \
#     .option("password", "pass123") \
#     .load()
    
# df.show()

conf = SparkConf().setMaster("local").setAppName("Spark Context")
sc = SparkContext(conf = conf)

nums = sc.parallelize([1, 2, 3, 4])

# map() transformation

new_nums = nums.map(lambda x: x*2)
print(new_nums.collect())