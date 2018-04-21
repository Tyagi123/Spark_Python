from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('APPLE_STOCK').getOrCreate()

data = spark.read.csv("./Data/HistoricalQuotes.csv", inferSchema=True, header=True)

data.printSchema()

'''data.show()'''

print(data.head(3))


def select_column_data(condition_column, condition, selected_column):
    data1 = spark.read.csv("./Data/HistoricalQuotes.csv",
                           inferSchema=True, header=True)
    return data1.filter(data1[condition_column] < condition).select(selected_column)


select_column_data("open", 171, "volume").show()

result = data.filter((data["open"] > 78) & (data["high"] < 170)).collect()

print(result)

row = result[0]

print(row.asDict()["volume"])
