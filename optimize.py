'''
Optimize the query plan

Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

import os


spark = SparkSession.builder.appName('Optimize I').getOrCreate()

# base_path = os.getcwd()
#
# project_path = ('/').join(base_path.split('/')[0:-3])
#
# answers_input_path = os.path.join(project_path, 'data/answers')
#
# questions_input_path = os.path.join(project_path, 'data/questions')

# retrieve from current working directory
answersDF = spark.read.option('path', 'data/answers').load()

questionsDF = spark.read.option('path', 'data/questions').load()

'''
Answers aggregation

Here we : get number of answers per question per month
'''

answers_month = answersDF.withColumn('month', month('creation_date'))\
    .groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id')\
    .select('question_id', 'creation_date', 'title', 'month', 'cnt')\
    .orderBy('question_id', 'month')

out_1 = resultDF.collect()
'''
Task:

see the query plan of the previous result and rewrite the query to optimize it
'''

'''
Optimized plan

    1) The only material performance improvement I could measure on my stand-alone spark cluster was
    to reduce the number of shuffle partitions to 4.
    2) I also filtered for the needed fields immediately, but could not identify any benefits in the consequent plans.
    
    I also experimented with the following scenarios:
        - Join before aggregation (in the hope that Spark would realize efficiencies my doing the and order by on a single
        table.
        - Repartition the answer dataframe by question_id or question_id and month.
        - Load the parquet files as tables to see if they had any metadata to leverage.
        
    See explore.ipynb to see the experiments. 

'''

spark.conf.set('spark.sql.shuffle.partitions', 4)

answersDF_2 = spark.read.option('path', 'data/answers').load().select(['question_id', 'creation_date'])
questionsDF_2 = spark.read.option('path', 'data/questions').load() \
    .select(['question_id', 'title', 'creation_date'])

answers_month_2 = answersDF_2.withColumn('month', month('creation_date')) \
    .groupBy('question_id', 'month') \
    .agg(count('*').alias('cnt')) \

resultDF_2 = questionsDF_2.join(answers_month_2, 'question_id') \
    .select('question_id', 'creation_date', 'title', 'month', 'cnt') \
    .orderBy('question_id', 'month')

out_2 = resultDF_2.collect()



