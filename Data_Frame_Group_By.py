from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GroupBy").getOrCreate()
data = spark.read.csv("./Data/constituents_csv.csv", header=True, inferSchema=True)

data.printSchema()

data.show()

data.groupBy("Sector").count().show()

'''PYSPARK function to count distinct'''
from pyspark.sql.functions import countDistinct

data.select(countDistinct("Sector")).show()
