# Databricks notebook source
# MAGIC %md
# MAGIC #Silver Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
  .option("header", True)\
  .option("inferSchema", True)\
  .load("abfss://bronze@netflixprojectdlsubhik.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Removing nulls by relacing it to 0
# MAGIC df.withColumn()  - used to create a new column or modify the exsisting column.
# MAGIC We need library pyspark.sql.functions
# MAGIC df.fillna()<br>
# MAGIC df = df.fillna(0) - applies 0 all place where it is null<br>
# MAGIC df = df.fillna(0, set = column names) - 1 value can be applied to diffrent column<br>
# MAGIC df = df.fillna({key:val, key:val}) - different value for different key<br>
# MAGIC

# COMMAND ----------

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Change datatype to int
# MAGIC df = df.withColumn(column_name, transformation )
# MAGIC
# MAGIC we can give multiple transformation in single part using \.functionName()

# COMMAND ----------

df = df.withColumn("duration_minutes", col('duration_minutes').cast(IntegerType()))\
    .withColumn("duration_seasons", col('duration_seasons').cast(IntegerType()))

# COMMAND ----------

# MAGIC %md
# MAGIC To check whether datatype modified

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC From titles column only the data before colun(:) to be taken
# MAGIC suing stting function split and index value we are accessing the first value before colon

# COMMAND ----------

df = df.withColumn("shorttitle",split(col('title'),':')[0])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC We want to take the value rating before a hypen(-)

# COMMAND ----------

df = df.withColumn("rating",split(col('rating'),'-')[0])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC For the column type. 
# MAGIC as it has only two value Movie, TV Show (Will create a boolean values (a conditional case)) 0,1
# MAGIC
# MAGIC Case when/
# MAGIC When otherwise statement
# MAGIC

# COMMAND ----------

# df = df.withColumn("type_flag",when(col('type') == 'Movie',1).otherWise(0))
df = df.withColumn("type_flag",when(col('type') == 'Movie',1)\
  .when(col('type')=='TV Show',2)\
  .otherwise(0))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Rank data for the duration_minutes<br>
# MAGIC Dense rank so same column same number

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

df = df.withColumn("duration_ranking", dense_rank().over(Window.orderBy(col('duration_minutes').desc())))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Without using pyspark we can use SQL too
# MAGIC temporary view is created which is local to this notebook can't be used in another notebook

# COMMAND ----------

#creation of temporary table
df.createOrReplaceTempView("tempview")


# COMMAND ----------

# this can be used outside the notebook but will be deleted if session is terminated
df.createGlobalTempView("globalTempView")

# COMMAND ----------

df = spark.sql(
    """
    SELECT * FROM global_temp.globalTempView;
    """
)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC To find How many Movies and TV Show is present (type column)

# COMMAND ----------

df_visual = df.groupBy("type").agg(count("*").alias("total_count"))
df_visual.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@netflixprojectdlsubhik.dfs.core.windows.net/netflix_titles")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC To check this go to ADLS> silver container>Netflix_titles

# COMMAND ----------

# MAGIC %md
# MAGIC (Workflows). This master data has to be run only on sunday.
# MAGIC

# COMMAND ----------

