from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('NLP').getOrCreate()

from pyspark.ml.feature import Tokenizer, RegexTokenizer

from pyspark.sql.functions import col, udf

from pyspark.sql.types import IntegerType

data = spark_session.createDataFrame([(0, 'Hello This is California USA'),
                                      (1, 'Hi This is USA'),
                                      (2, 'USA,ENg,America,Japan')],
                                     ['id', 'sentence'])

print(data.show())

tokenizer = Tokenizer(inputCol='sentence', outputCol='words')

regex_tokenizer = RegexTokenizer(inputCol='sentence', outputCol='words', pattern='\\W')

count_tokens = udf(lambda words: len(words), IntegerType())

tokenized = tokenizer.transform(data)

print(tokenized.show())

print(tokenized.withColumn('tokens', count_tokens(col('words'))).show())

regex_tokenized = regex_tokenizer.transform(data)

print(regex_tokenized.show())

print(regex_tokenized.withColumn('tokens', count_tokens(col('words'))).show())
