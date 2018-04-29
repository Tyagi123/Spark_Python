from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName("LP").getOrCreate()

data = spark_session.read.csv("./Data/RealEstate.csv", header=True, inferSchema=True)

''' to show data'''

data.show()

'''to show column'''
print(data.columns)

'''Group by count to check how many diff location data has'''
print(data.groupBy('Location').count().show())

'''Categorise String data to numeric data for location '''
indexer = StringIndexer(inputCol='Location', outputCol='Location_Cat')

indexed_location = indexer.fit(data).transform(data)

print(indexed_location.show())

'''Vector assemble - assign input and output column'''
assembler = VectorAssembler(inputCols=['MLS', 'Bedrooms', 'Bathrooms', 'Size', 'Location_Cat', 'Price per sqft'],
                            outputCol='forecast_price')

output = assembler.transform(indexed_location);

print(output.head(1))

'''final data with forecast variable and price'''
final_data = output.select('forecast_price', 'price')

print(final_data.show())

'''Split data into training and test data '''
training_data, test_data = final_data.randomSplit([0.7, 0.3])

print(training_data.describe().show())

print(test_data.describe().show())

'''Start liner regression'''
lr = LinearRegression(labelCol='price', featuresCol='forecast_price')

lr_model = lr.fit(training_data)

test_result = lr_model.evaluate(test_data)

print(test_result.residuals.show())

print(test_data.describe().show())

'''Mean squared error'''
print(test_result.rootMeanSquaredError)

'''root error'''
print(test_result.r2)

'''Mean absolute error'''
print(test_result.meanAbsoluteError)

''' Check Corrlation ship between 2 fields '''
from pyspark.sql.functions import corr

print(data.select(corr('Bathrooms', 'Price')).show())
