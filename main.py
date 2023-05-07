from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql import Window

spark_session = (SparkSession.builder
    .master("local")
    .appName("task app")
    .config(conf=SparkConf())
    .getOrCreate())

schema_title_akas = t.StructType([
    t.StructField("titleId", t.StringType(), nullable=False),
    t.StructField("ordering", t.StringType(), nullable=False),
    t.StructField("title", t.StringType(), nullable=False),
    t.StructField("region", t.StringType(), nullable=True),
    t.StructField("language", t.StringType(), nullable=True),
    t.StructField("types", t.StringType(), nullable=True),
    t.StructField("attributes", t.StringType(), nullable=True),
    t.StructField("isOriginalTitle", t.StringType(), nullable=True)
    ])
schema_name_basics = t.StructType([
    t.StructField("nconst", t.StringType(), nullable=True),
    t.StructField("primaryName", t.StringType(), nullable=False),
    t.StructField("birthYear", t.IntegerType(), nullable=True),
    t.StructField("deathYear", t.IntegerType(), nullable=True),
    t.StructField("primaryProfession", t.StringType(), nullable=True),
    t.StructField("knownForTitles", t.StringType(), nullable=True)
    ])
schema_title_basics = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=True),
    t.StructField("titleType", t.StringType(), nullable=True),
    t.StructField("primaryTitle", t.StringType(), nullable=True),
    t.StructField("originalTitle", t.StringType(), nullable=True),
    t.StructField("isAdult", t.StringType(), nullable=True),
    t.StructField("startYear", t.IntegerType(), nullable=True),
    t.StructField("endYear", t.IntegerType(), nullable=True),
    t.StructField("runtimeMinutes", t.IntegerType(), nullable=True),
    t.StructField("genres", t.StringType(), nullable=True),
])
schema_title_principals = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=True),
    t.StructField("ordering", t.StringType(), nullable=True),
    t.StructField("nconst", t.StringType(), nullable=True),
    t.StructField("category", t.StringType(), nullable=True),
    t.StructField("job", t.StringType(), nullable=True),
    t.StructField("characters", t.StringType(), nullable=True)
    ])
schema_title_name = t.StructType([
    t.StructField("nconst", t.StringType(), nullable=True),
    t.StructField("primaryName", t.StringType(), nullable=False),
    t.StructField("birthYear", t.IntegerType(), nullable=True),
    t.StructField("deathYear", t.IntegerType(), nullable=True),
    t.StructField("primaryProfession", t.StringType(), nullable=True),
    t.StructField("knownForTitles", t.StringType(), nullable=True)
    ])
schema_ratings_basics = t.StructType([
    t.StructField("tconst", t.StringType(), nullable=True),
    t.StructField("averageRating", t.DoubleType(), nullable=True),
    t.StructField("numVotes", t.IntegerType(), nullable=True)
])
schema_title_episode = t.StructType([
        t.StructField("tconst", t.StringType(), nullable=True),
        t.StructField("parentTconst", t.StringType(), nullable=True),
        t.StructField("seasonNumber", t.IntegerType(), nullable=True),
        t.StructField("episodeNumber", t.IntegerType(), nullable=True),
    ])

file_read_title_akas = r'.\input\title.akas.tsv.gz'
file_read_name_basics = r'.\input\name.basics.tsv.gz'
file_read_title_basics = r'.\input\title.basics.tsv.gz'
file_read_title_principals = r'.\input\title.principals.tsv.gz'
file_read_title_ratings = r'.\input\title.ratings.tsv.gz'
file_read_title_episode = r'.\input\title.episode.tsv.gz'

title_akas_df = spark_session.read.csv(file_read_title_akas, header=True,
                                    nullValue='null', sep=r'\t', schema=schema_title_akas)
name_basics_df = spark_session.read.csv(file_read_name_basics, header=True,
                                    nullValue='null', sep=r'\t', schema=schema_name_basics)
title_basics_df = spark_session.read.csv(file_read_title_basics, header=True,
                                    nullValue='null', sep=r'\t', schema=schema_title_basics)
title_principals_df = spark_session.read.csv(file_read_title_principals, header=True,
                                    nullValue='null', sep=r'\t', schema=schema_title_principals)
ratings_basics_df = spark_session.read.csv(file_read_title_ratings, header=True,
                                    nullValue='null', sep=r'\t', schema=schema_ratings_basics)
title_episode_df = spark_session.read.csv(file_read_title_episode, header=True,
                                    nullValue='null', sep=r'\t', schema=schema_title_episode)

""" Task N1
Get all titles of series/movies etc. that are available in Ukrainian.
Отримайте всі назви серіалів/фільмів тощо, доступні українською мовою.
"""
df_task1 = title_akas_df.select("title", "region").where(f.col("region") == "UA")
df_task1.show(100, truncate=False)
file_write = r'.\output\task01'
df_task1.write.csv(file_write, header=True, mode="overwrite")

""" Task N2
Get the list of peopleʼs names, who were born in the 19th century.
Отримайте список імен людей, які народилися в 19 столітті.
"""
#df_task2 = name_basics_df.select("primaryName", "birthYear")\
#    .filter((f.col("birthYear") < 1900) & (f.col("birthYear") > 1799))
#df_task2.show(100, truncate=False)
#file_write = r'.\output\task02'
#df_task2.write.csv(file_write, header=True, mode="overwrite")

""" Task N3
Get titles of all movies that last more than 2 hours.
Отримайте назви всіх фільмів, які тривають більше 2 годин.
"""
#df_task3 = title_basics_df.select("titleType", "originalTitle", "runtimeMinutes")\
#    .filter((f.col("runtimeMinutes") > 120) & (f.col("titleType").isin("movie", "tvMovie")))
#df_task3.show(200, truncate=False)
#file_write = r'.\output\task03'
#df_task3.write.csv(file_write, header=True, mode="overwrite")

""" Task N4
Get names of people, corresponding movies/series and characters they played in those films.
Отримайте імена людей, відповідні фільми/серіали та персонажівграв у тих фільмах.
"""
#df_task41 = title_principals_df.select("tconst", "nconst", "characters").filter(f.col("characters") != r"\N")
#df_task42 = name_basics_df.select("nconst", "primaryName")
#df_task43 = df_task41.join(df_task42, "nconst")
#df_task44 = title_basics_df.select("tconst", "primaryTitle")
#df_task4 = df_task43.join(df_task44, "tconst")\
#    .select("primaryName", "characters", "primaryTitle").orderBy("primaryName")
#df_task4.show(100, truncate=False)
#file_write = r'.\output\task04'
#df_task4.write.csv(file_write, header=True, mode="overwrite")

""" Task №5
Get information about how many adult movies/series etc. there are per
region. Get the top 100 of them from the region with the biggest count to
the region with the smallest one.
Отримайте інформацію про кількість фільмів/серіалів для дорослих тощо на
область. Отримайте 100 найкращих із регіону з найбільшою кількістю
регіон з найменшим.
"""
#temp_df51 = title_basics_df.select("tconst", "isAdult").filter(f.col("isAdult") == 1)
#temp_df52 = title_akas_df.select("region", "titleId", "title")\
#    .filter((f.col("region").isNotNull()) & (f.col("region") != r"\N")).withColumnRenamed("titleId", "tconst")
#temp_df53 = temp_df51.join(temp_df52, "tconst")
#temp_df54 = temp_df53.join(ratings_basics_df.select("averageRating", "tconst"), "tconst")
#window = Window.partitionBy("region").orderBy("region")
#temp_df54 = temp_df54.withColumn("adult_per_region", f.count(f.col("region")).over(window))
#min_region = temp_df54.agg(f.min("adult_per_region")).collect()[0][0]
#max_region = temp_df54.agg(f.max("adult_per_region")).collect()[0][0]
#df_min = temp_df54.filter(f.col("adult_per_region") == min_region ).orderBy(f.col("averageRating").desc()).limit(100)
#df_max = temp_df54.filter(f.col("adult_per_region") == max_region).orderBy(f.col("averageRating").desc()).limit(100)
#df_task5 = df_min.union(df_max).select("region", "title", "averageRating", "adult_per_region")
#df_task5.show(200, truncate=False)
#file_write = r'.\output\task05'
#df_task5.write.csv(file_write, header=True, mode="overwrite")
"""  Task №6
Get information about how many episodes in each TV Series. Get the top
50 of them starting from the TV Series with the biggest quantity of
episodes.
Отримайте інформацію про кількість епізодів у кожному серіалі. Отримайте вершину
Їх 50, починаючи з серіалу з найбільшою кількістю епізоди.
"""
#temp_df61 = title_episode_df.groupBy("parentTconst").count().orderBy(f.desc("count")).limit(50)
#temp_df61 = temp_df61.withColumnRenamed("parentTconst", "tconst")
#temp_df62 = title_basics_df.select("tconst", "titleType", "originalTitle").filter(f.col("titleType") == "tvSeries")
#df_task6 = temp_df61.join(temp_df62, "tconst").select("originalTitle", "titleType", "count").orderBy(f.desc("count"))
#df_task6.show(100, truncate=False)
#file_write = r'.\output\task06'
#df_task6.write.csv(file_write, header=True, mode="overwrite")
"""   Task №7
Get 10 titles of the most popular movies/series etc. by each decade.
Отримайте 10 назв найпопулярніших фільмів/серіалів тощо за кожне десятиліття.
"""
#temp_df71 = title_basics_df.select("tconst", "originalTitle", "startYear").filter(f.col("startYear").isNotNull())
#temp_df72 = ratings_basics_df.select("tconst", "averageRating")
#temp_df73 = temp_df71.join(temp_df72, "tconst")
#temp_df73 = temp_df73.select("originalTitle", "startYear", "averageRating")\
#    .withColumn("N_decade", f.floor(f.col("startYear") / 10).cast(t.IntegerType()))
#window = Window.partitionBy("N_decade").orderBy(f.desc("averageRating"))
#df_task7 = temp_df73.withColumn("Rating_decade", f.row_number().over(window)).where(f.col("Rating_decade") <= 10)
#df_task7.show(100, truncate=False)
#file_write = r'.\output\task07'
#df_task7.write.csv(file_write, header=True, mode="overwrite")
"""   Task №8
Get 10 titles of the most popular movies/series etc. by each genre.
Отримайте 10 назв найпопулярніших фільмів/серіалів тощо за кожним жанром.
"""
#temp_df81 = title_basics_df.withColumn("genres", f.explode(f.split(f.col("genres"), ",")))
#temp_df81 = temp_df81.select("tconst", "titleType", "primaryTitle", "genres")
#temp_df82 = ratings_basics_df.select("tconst", "averageRating")
#temp_df83 = temp_df81.join(temp_df82, "tconst")
#window = (Window.orderBy(f.desc("genres"), f.desc("averageRating")).partitionBy("genres"))
#df_task8 = temp_df83.withColumn("Rating_genre", f.row_number().over(window)).where(f.col("Rating_genre") <= 10)
#df_task8.show(100, truncate=False)
#file_write = r'.\output\task08'
#df_task8.write.csv(file_write, header=True, mode="overwrite")
