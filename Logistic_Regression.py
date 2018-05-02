from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('Logistic_App').getOrCreate()

data = spark_session.read.csv('./Data/Admission_Stats.csv', header=True, inferSchema=True)

print(data.show())

assembler = VectorAssembler(inputCols=['gre', 'gpa', 'rank'], outputCol='features')
output_data = assembler.transform(data)

training_data, test_data = output_data.randomSplit([0.7, 0.3])

lr_model = LogisticRegression(featuresCol='features', labelCol='admit')

lr_final = lr_model.fit(training_data)

test_result = lr_final.evaluate(test_data)
