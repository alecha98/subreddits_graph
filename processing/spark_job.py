import copy
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

# Connecting to DB
SparkSession.builder.config('spark.jars', '/usr/local/spark/jars/postgresql-42.2.13.jar') 
__db_url = "jdbc:postgresql://10.0.0.14:5432/subreddits"
__table_name = "subreddits_l"
__properties = {
  "driver": "org.postgresql.Driver",
  "user": 'postgres',
  "password": os.enciron.get('db_password')
}
__write_mode = 'overwrite'


region = 'us-west-2'
bucket = 'indightreddit'

#Configure spark 
sc = SparkContext()
sc.setLogLevel("WARN")
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)

#Reading files
s3file = f's3a://{bucket}/*'

df = spark.read.format('json').load(s3file)

#Selecting only nessesary data
all_data = df\
            .select(['subreddit', 'author'])\
            .dropDuplicates()
#Cleaning from irreevant information
all_data = all_data.filter(all_data.author != "[deleted]").cache()

#Selecting the most popular subreddits
subreddit_authors = all_data\
            .groupBy('subreddit').count()\
            .orderBy(col('count').desc()).limit(300)
#Creating table with only names of subreddits
subreddits1 = subreddit_authors.select(col("subreddit").alias("subreddit1"))

#Cleaning all_data from all non-top subreddits
all_data = subreddits1\
    .join(all_data, subreddits1.subreddit1 == all_data.subreddit, how='left')\
    .select(["subreddit", "author"])
subreddits2 = subreddits1.select(col("subreddit1").alias("subreddit2"))

#Creating table with all pairs of most popular subreddits and adding adding all authors who was mentionet at least it one
subreddits_pairs = subreddits1\
    .crossJoin(subreddits2)\
    .filter(subreddits1.subreddit1 < subreddits2.subreddit2)\
    .join(all_data,\
            (subreddits1.subreddit1 == all_data.subreddit) | \
            (subreddits2.subreddit2 == all_data.subreddit))\
    .select(["subreddit1", "subreddit2", "author"])

#Count all authors in each pair
all_authors = subreddits_pairs.dropDuplicates().groupBy("subreddit1", "subreddit2").count()
all_authors = all_authors.select(["subreddit1", "subreddit2", "count"])

#Count common authors in pairs
result = all_authors\
    .join(subreddit_authors\
    .withColumnRenamed('count', 'sub1_count'), all_authors.subreddit1 == subreddit_authors.subreddit, how='left')\
    .drop('subreddit')\
    .join(subreddit_authors.withColumnRenamed('count', 'sub2_count'),\
            all_authors.subreddit2 == subreddit_authors.subreddit, how='left')\
    .drop('subreddit')
result = result.filter((col('sub1_count') + col('sub2_count') - col('count')) >= 10)
result = result\
    .withColumn('metric', (col('sub1_count') + col('sub2_count') - col('count'))/ col('count'))\
    .select(['subreddit1', 'subreddit2', 'metric'])

#Writing to DB
result.write.jdbc(url=__db_url,
           table=__table_name,
            mode=__write_mode,
           properties=__properties)
spark.stop()
