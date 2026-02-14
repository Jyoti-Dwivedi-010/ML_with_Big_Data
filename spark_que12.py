from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract

# Initialize Spark with Strict Memory Limits (Anti-Crash)
spark = SparkSession.builder \
    .appName("M25CSA010_Q12_Influence_Network") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

#  Load the dataset
# minPartitions=50 ensures files are streamed into memory in tiny chunks
raw_files = spark.sparkContext.wholeTextFiles("/user/jyoti/*.txt", minPartitions=50)
books_df = raw_files.map(lambda x: (x[0].split("/")[-1], x[1])).toDF(["file_name", "text"])

# Extract Metadata via Regex
author_regex = r"Author:\s*([^\n\r]+)"
year_regex = r"Release Date:.*?\b(\d{4})\b"

metadata_df = books_df.withColumn("author", regexp_extract(col("text"), author_regex, 1)) \
                      .withColumn("year", regexp_extract(col("text"), year_regex, 1).cast("int"))

# Clean up: Remove rows where author or year couldn't be found, and drop duplicates
authors_df = metadata_df.select("author", "year") \
                        .filter((col("author") != "") & col("year").isNotNull()) \
                        .distinct()

# Construct the Influence Network (X = 5 years)
X = 5

# Create aliases for the self-join
a1 = authors_df.alias("a1") # Potential Influencer
a2 = authors_df.alias("a2") # Potential Influenced

#  a1 influenced a2 IF a1 published before a2, AND within X years
influence_edges = a1.join(a2, 
    (col("a1.author") != col("a2.author")) &          # Must be different authors
    (col("a2.year") >= col("a1.year")) &              # a2 published after (or same year as) a1
    ((col("a2.year") - col("a1.year")) <= X)          # Within the 5-year window
).select(
    col("a1.author").alias("influencer"), 
    col("a2.author").alias("influenced")
).cache()

print("\n" + "="*50)
print("NETWORK EDGES COMPUTED SUCCESSFULLY")
print("="*50)

# Network Analysis: Degrees
# IN-DEGREE: How many authors influenced YOU
in_degree = influence_edges.groupBy("influenced").count() \
                           .withColumnRenamed("count", "in_degree") \
                           .orderBy(col("in_degree").desc())

print("\n" + "="*50)
print("TOP 5 AUTHORS BY IN-DEGREE (Most Influenced by others)")
print("="*50)
in_degree.show(5, truncate=False)

# OUT-DEGREE: How many authors YOU influenced
out_degree = influence_edges.groupBy("influencer").count() \
                            .withColumnRenamed("count", "out_degree") \
                            .orderBy(col("out_degree").desc())

print("\n" + "="*50)
print("TOP 5 AUTHORS BY OUT-DEGREE (Most Influential to others)")
print("="*50)
out_degree.show(5, truncate=False)

spark.stop()