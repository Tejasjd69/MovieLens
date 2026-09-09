# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;

# COMMAND ----------

ratings = spark.read.text("file:/Workspace/Users/tejasjd123@gmail.com/Drafts/ratings.dat")
ratings.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split, col, count

base_path = "file:/Workspace/Users/tejasjd123@gmail.com/Drafts/"

# Load ratings and split the :: delimited fields
ratings_raw = spark.read.text(base_path + "ratings.dat")
ratings = ratings_raw.select(split(col("value"), "::").alias("fields")).select(
    col("fields")[0].alias("UserID"),
    col("fields")[1].cast("int").alias("MovieID"),
    col("fields")[2].alias("Rating"),
    col("fields")[3].alias("Timestamp")
)

# Count ratings per movie, get top 10
top10 = ratings.groupBy("MovieID").agg(count("*").alias("ViewCount")) \
    .orderBy(col("ViewCount").desc()) \
    .limit(10)

# Load movies and split the :: delimited fields
movies_raw = spark.read.text(base_path + "movies.dat")
movies = movies_raw.select(split(col("value"), "::").alias("fields")).select(
    col("fields")[0].cast("int").alias("MovieID"),
    col("fields")[1].alias("Title")
)

# Join top 10 movie IDs with their titles
result = top10.join(movies, on="MovieID").orderBy(col("ViewCount").desc())
result.select("Title", "ViewCount").show(10, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split, col, explode

base_path = "file:/Workspace/Users/tejasjd123@gmail.com/Drafts/"

movies_raw = spark.read.text(base_path + "movies.dat")

# Split each line on "::" to get the genres field (3rd column)
movies_split = movies_raw.select(split(col("value"), "::").alias("fields"))
genres_col = movies_split.select(col("fields")[2].alias("genres_raw"))

# Genres are pipe-separated within that field (e.g. "Animation|Children's|Comedy")
# explode() turns each pipe-separated genre into its own row
genres_exploded = genres_col.select(explode(split(col("genres_raw"), "\\|")).alias("genre"))

# Get distinct genres, sorted alphabetically
distinct_genres = genres_exploded.distinct().orderBy("genre")
distinct_genres.show(30, truncate=False)

# COMMAND ----------

base_path = "file:/Workspace/Users/tejasjd123@gmail.com/Drafts/"

# Read raw files as text and create temp views
spark.read.text(base_path + "movies.dat").createOrReplaceTempView("movies_staging")
spark.read.text(base_path + "ratings.dat").createOrReplaceTempView("ratings_staging")
spark.read.text(base_path + "users.dat").createOrReplaceTempView("users_staging")

# Create a database (schema) to store the tables
spark.sql("DROP DATABASE IF EXISTS sparkdatalake CASCADE")
spark.sql("CREATE DATABASE sparkdatalake")

# Movies table: parse the :: delimited fields and extract year from title
spark.sql("""
    SELECT 
        split(value, '::')[0] AS movieid,
        split(value, '::')[1] AS title,
        substring(split(value, '::')[1], length(split(value, '::')[1]) - 4, 4) AS year,
        split(value, '::')[2] AS genre
    FROM movies_staging
""").write.mode("overwrite").saveAsTable("sparkdatalake.movies")

# Users table
spark.sql("""
    SELECT
        split(value, '::')[0] AS userid,
        split(value, '::')[1] AS gender,
        split(value, '::')[2] AS age,
        split(value, '::')[3] AS occupation,
        split(value, '::')[4] AS zipcode
    FROM users_staging
""").write.mode("overwrite").saveAsTable("sparkdatalake.users")

# Ratings table
spark.sql("""
    SELECT
        split(value, '::')[0] AS userid,
        split(value, '::')[1] AS movieid,
        split(value, '::')[2] AS rating,
        split(value, '::')[3] AS timestamp
    FROM ratings_staging
""").write.mode("overwrite").saveAsTable("sparkdatalake.ratings")

print("Done — sparkdatalake database created with movies, users, ratings tables")

# COMMAND ----------

spark.sql("SHOW TABLES IN sparkdatalake").show()
spark.sql("SELECT * FROM sparkdatalake.movies LIMIT 5").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import split, col, explode, count

base_path = "file:/Workspace/Users/tejasjd123@gmail.com/Drafts/"

movies_raw = spark.read.text(base_path + "movies.dat")
movies_split = movies_raw.select(split(col("value"), "::").alias("fields"))
genres_col = movies_split.select(col("fields")[2].alias("genres_raw"))

# Explode pipe-separated genres into individual rows
genres_exploded = genres_col.select(explode(split(col("genres_raw"), "\\|")).alias("genre"))

# Count movies per genre, sorted alphabetically by genre
genre_count = genres_exploded.groupBy("genre").agg(count("*").alias("movie_count")).orderBy("genre")
genre_count.show(30, truncate=False)

# COMMAND ----------

avg_ratings = spark.sql("""
    SELECT 
        movieid,
        CAST(AVG(rating) AS DECIMAL(16,2)) AS Average_ratings
    FROM sparkdatalake.ratings
    GROUP BY movieid
    ORDER BY CAST(movieid AS INT) ASC
""")

avg_ratings.show(20, truncate=False)

# COMMAND ----------

base_path = "file:/Workspace/Users/tejasjd123@gmail.com/Drafts/"

movies_raw = spark.read.text(base_path + "movies.dat")
movies_raw.createOrReplaceTempView("movies_view")

spark.sql("""
    SELECT
        split(value, '::')[0] AS movieid,
        split(value, '::')[1] AS moviename,
        substring(split(value, '::')[1], length(split(value, '::')[1]) - 4, 4) AS year
    FROM movies_view
""").createOrReplaceTempView("movies")

oldest = spark.sql("""
    SELECT * FROM movies m1 
    WHERE m1.year = (SELECT MIN(m2.year) FROM movies m2)
""")

oldest.show(50, truncate=False)

# COMMAND ----------

oldest = spark.sql("""
    SELECT 
        movieid,
        moviename,
        CAST(regexp_extract(moviename, '\\\\((\\\\d{4})\\\\)', 1) AS INT) AS year
    FROM movies
""")

oldest.createOrReplaceTempView("movies_clean")

result = spark.sql("""
    SELECT * FROM movies_clean m1
    WHERE m1.year = (SELECT MIN(year) FROM movies_clean)
""")

result.show(50, truncate=False)