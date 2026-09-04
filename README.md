MovieLens Analytics — PySpark & Hive Data Warehouse

A big data analytics pipeline built on the MovieLens 1M dataset, using PySpark and Spark SQL to transform raw delimited text data into a queryable Hive-backed data warehouse. The project covers the full flow from raw data ingestion through schema design, table creation, and analytical querying — run on Databricks.

Adapted and reimplemented in PySpark from the original Scala/Spark project by Thomas George. All code was rewritten from Scala/RDD to PySpark/DataFrame APIs to run on Databricks' serverless compute (which does not support Scala or RDDs), and extended with additional data-quality fixes described below.

Dataset

MovieLens 1M — 1 million ratings from 6,000 users on 3,900 movies.

File	Description
movies.dat	MovieID, Title (with year), Genres (pipe-separated)
ratings.dat	UserID, MovieID, Rating, Timestamp
users.dat	UserID, Gender, Age, Occupation, Zipcode

All source files use :: as a field delimiter.

Architecture
┌─────────────────┐
│  Raw .dat Files  │   movies.dat / ratings.dat / users.dat  (:: delimited)
└────────┬─────────┘
         │  spark.read.text()
         ▼
┌─────────────────┐
│   Temp Views     │   movies_staging / ratings_staging / users_staging
└────────┬─────────┘
         │  Spark SQL — split() + substring/regexp_extract on `value` column
         ▼
┌─────────────────────────┐
│  Parsed & Typed Columns  │   movieid, title, year, genre, userid, rating, etc.
└────────┬─────────────────┘
         │  .write.mode("overwrite").saveAsTable()
         ▼
┌─────────────────────────────┐
│   Hive Metastore Database    │   sparkdatalake.movies
│   (Managed Tables)            │   sparkdatalake.users
│                                │   sparkdatalake.ratings
└────────┬──────────────────────┘
         │  spark.sql("SELECT ... FROM sparkdatalake.<table>")
         ▼
┌─────────────────────────┐
│   Analytical Queries      │   Top 10 movies, avg rating/movie,
│                            │   movies/genre, movies/year, oldest movies
└───────────────────────────┘

Flow summary: Raw text files are read as unstructured DataFrames and registered as temporary views. Spark SQL string transformations (split, substring/regexp_extract) parse the delimited fields into typed columns. The parsed DataFrames are then persisted as managed Hive tables under a dedicated sparkdatalake database via saveAsTable(), making them queryable through standard SQL for any downstream analysis — decoupling the one-time parsing/ETL step from repeated analytical querying.

Tech Stack
PySpark (DataFrame API) — used in place of Scala/RDDs, since Databricks' free-tier serverless compute supports only Python DataFrames, not Scala or RDDs
Spark SQL — schema parsing, aggregations, and joins
Hive — managed database and tables via the Spark-integrated Hive metastore
Databricks (serverless compute)
What This Pipeline Does
Ingestion — reads all three raw .dat files as text and stages them as temporary SQL views.
Schema design & transformation — parses each ::-delimited row into typed columns (e.g. extracting year from the movie title string) using Spark SQL.
Warehousing — creates a Hive database (sparkdatalake) and writes three managed tables: movies, users, ratings.
Analytics — runs SQL queries directly against the Hive tables to answer questions such as:
Top 10 most-rated movies
Average rating per movie
Number of movies released per year
Number of movies per genre
Oldest movie(s) in the dataset
Data Quality Fix: Year Extraction Bug

The initial year-extraction logic pulled the release year from the movie title using a fixed-offset substring (last 4 characters before the closing parenthesis). This worked for the vast majority of rows but silently broke on the title "Alien³ (1992)" — the superscript ³ character shifted the character count for that row, producing a malformed value ("(199") instead of a valid year.

Because year was stored as a string, an unqualified MIN() aggregation performed lexicographic (alphabetical) rather than numeric comparison. Since ( sorts before any digit in standard character ordering, the corrupted value was incorrectly returned as the "oldest" year.

Fix: replaced the fixed-offset substring with regexp_extract() to pull the 4-digit year directly from within the parentheses regardless of title length or special characters, and explicitly cast the result to INT before aggregation:

sql
CAST(regexp_extract(moviename, '\\((\\d{4})\\)', 1) AS INT) AS year

After the fix, the query correctly returns the dataset's actual oldest titles — three films from 1919.

This highlights a broader lesson applied throughout: fixed-offset string parsing is fragile against real-world text variation, and type-aware aggregation matters as much as correct extraction logic.

Running This Project
Upload movies.dat, ratings.dat, users.dat to your Databricks workspace (Volumes or Workspace Files).
Open a notebook with the Python language and attach it to a cluster.
Update the base_path variable in each script to point to your uploaded file location.
Run the scripts in order:
Ingestion/exploration queries (top movies, genres)
sparkdatalake table creation script (creates the Hive database and tables)
Analytical queries against the Hive tables (average rating, movies per year, oldest movies)
