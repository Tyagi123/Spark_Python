from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName("LP").getOrCreate()

data = spark_session.read.csv("./Data/RealEstate.csv", header=True, inferSchema=True)

''' to show data'''

data.show()

'''to show column'''
print(data.columns)

assembler = VectorAssembler(inputCols=['Bedrooms', 'Bathrooms', 'Size'],
                            outputCol='forecast_price')

output = assembler.transform(data);

print(output.head(1))

final_data = output.select('forecast_price', 'price')

print(final_data.show())

training_data, test_data = final_data.randomSplit([0.7, 0.3])

print(training_data.describe().show())

print(test_data.describe().show())

lr = LinearRegression(labelCol='price', featuresCol='forecast_price')

lr_model = lr.fit(training_data)

test_result = lr_model.evaluate(test_data)

print(test_result.residuals.show())

print(test_result.rootMeanSquaredError)

print(test_result.r2)
