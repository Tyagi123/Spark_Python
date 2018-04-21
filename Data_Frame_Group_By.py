from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GroupBy").getOrCreate()
data = spark.read.csv("./Data/HistoricalQuotes.csv")

data.printSchema()
