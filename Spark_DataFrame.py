from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basic').getOrCreate()

df = spark.read.json("./Data/generated.json", multiLine=True)

df.show()

df.printSchema()

print(df.columns)

print(df.describe().show())
