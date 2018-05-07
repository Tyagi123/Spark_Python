from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('NLP').getOrCreate()

from pyspark.ml.feature import Tokenizer, RegexTokenizer

from pyspark.sql.functions import col, udf

from pyspark.sql.types import IntegerType

'''Create Data frame for tokenizing'''
data = spark_session.createDataFrame([(0, 'Hello This is California USA'),
                                      (1, 'Hi This is USA'),
                                      (2, 'USA,ENg,America,Japan')],
                                     ['id', 'sentence'])

print(data.show())

'''Regular tokenizer will spilt by space '''
tokenizer = Tokenizer(inputCol='sentence', outputCol='words')

'''Regex tokenizer will spilt by patterns'''
regex_tokenizer = RegexTokenizer(inputCol='sentence', outputCol='words', pattern='\\W')

'''User define method to count words'''
count_tokens = udf(lambda words: len(words), IntegerType())

tokenized = tokenizer.transform(data)

print(tokenized.show(truncate=False))

print(tokenized.withColumn('tokens', count_tokens(col('words'))).show())

regex_tokenized = regex_tokenizer.transform(data)

print(regex_tokenized.show(truncate=False))

print(regex_tokenized.withColumn('tokens', count_tokens(col('words'))).show())

from pyspark.ml.feature import StopWordsRemover

'''Create data frame to remove repeated words'''
data1 = spark_session.createDataFrame([(0, ['Hello', 'This', 'is', 'California', 'USA']),
                                       (1, ['This', 'is', 'The', 'America'])],
                                      ['id', 'tokens'])

print(data1.show())

remover = StopWordsRemover(inputCol='tokens', outputCol='after_remove_repeat')

print(remover.transform(data1).show(truncate=False))

from pyspark.ml.feature import NGram

'''Data frame to spilt in list of words like n=2 NGram will create list with 2 words'''

ngram_data = spark_session.createDataFrame([(0, ['Hello', 'This', 'is', 'California', 'USA']),
                                            (1, ['This', 'is', 'The', 'America'])],
                                           ['id', 'tokens'])

ngram = NGram(inputCol='tokens', outputCol='ngram_output', n=2)

print(ngram.transform(ngram_data).show(truncate=False))

from pyspark.ml.feature import HashingTF, IDF

'''Data frame to split into hash tags (like vectors)'''
data3 = spark_session.createDataFrame([(0.0, 'Hello This is California USA'),
                                       (1.0, 'Hi This is USA'),
                                       (2.0, 'USA,ENg,America,Japan')],
                                      ['label', 'sentence'])

tokenizer_data3 = Tokenizer(inputCol='sentence', outputCol='words')

words_data3 = tokenizer_data3.transform(data3)

print(words_data3.show(truncate=False))

hashing = HashingTF(inputCol='words', outputCol='features')

hashing_data = hashing.transform(words_data3)

print(hashing_data.show(truncate=False))

idf = IDF(inputCol='features', outputCol='idf_features')

idf_data = idf.fit(hashing_data).transform(hashing_data)

print(idf_data.show(truncate=False))
