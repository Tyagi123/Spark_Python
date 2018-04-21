from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basic').getOrCreate()

df = spark.read.json("./Data/generated.json", multiLine=True)

df.printSchema()

from pyspark.sql.types import StructField, StringType, StructType

data_schema = [StructField("type", StringType(), False),
               StructField("number", StringType(), False)]

final_struct = StructType(fields=data_schema)

df = spark.read.json("./Data", multiLine=True, schema=final_struct)

df.printSchema()

df.select("type").show()
