from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, regexp_extract, when
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
import numpy as np

TARGET_BOOK = "10.txt"        
NUM_BOOKS_TO_COMPARE = 25     
HDFS_DIR = "/user/jyoti/"


spark = SparkSession.builder.appName(f"M25CSA010_Q11_Dynamic_{NUM_BOOKS_TO_COMPARE}").getOrCreate()
sc = spark.sparkContext

# We use Spark's Hadoop connection to scan the directory without reading file contents
Path = spark._jvm.org.apache.hadoop.fs.Path
FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(sc._jsc.hadoopConfiguration())

# Loop through the HDFS directory to collect filenames
all_files = []
statuses = fs.listStatus(Path(HDFS_DIR))
for status in statuses:
    file_name = status.getPath().getName()
    if file_name.endswith(".txt"):
        all_files.append(file_name)

# Filter out the target book, and slice the array to get exactly 'NUM_BOOKS_TO_COMPARE'
comparison_books = [f for f in all_files if f != TARGET_BOOK][:NUM_BOOKS_TO_COMPARE]

print("\n" + "="*50)
print(f"TARGET BOOK: {TARGET_BOOK}")
print(f"DYNAMICALLY FETCHED {len(comparison_books)} BOOKS FOR COMPARISON:")
print(comparison_books)
print("="*50 + "\n")

# Combine the target book with our fetched books and create the load string
books_to_load = [TARGET_BOOK] + comparison_books
files_to_load = ",".join([f"{HDFS_DIR}{book}" for book in books_to_load])

# Load ONLY the fetched files into Spark
raw_files = sc.wholeTextFiles(files_to_load)
books_df = raw_files.map(lambda x: (x[0].split("/")[-1], x[1])).toDF(["file_name", "text"])

# Preprocessing
extract_pattern = r"(?s)\*\*\* START OF THE PROJECT GUTENBERG EBOOK.*?\*\*\*(.*?)\*\*\* END OF THE PROJECT GUTENBERG EBOOK"
clean_df = books_df.withColumn("text_clean", regexp_extract(col("text"), extract_pattern, 1))
clean_df = clean_df.withColumn("text_clean", when(col("text_clean") == "", col("text")).otherwise(col("text_clean")))

clean_df = clean_df.withColumn("text_clean", lower(col("text_clean"))) \
                   .withColumn("text_clean", regexp_replace(col("text_clean"), r'[^a-z\s]', ' '))

#  TF-IDF Pipeline
tokenizer = Tokenizer(inputCol="text_clean", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=10000) 
idf = IDF(inputCol="tf", outputCol="tfidf")

# Fit and Transform
processed_df = idf.fit(hashingTF.transform(remover.transform(tokenizer.transform(clean_df)))) \
                  .transform(hashingTF.transform(remover.transform(tokenizer.transform(clean_df))))

target_row = processed_df.filter(col("file_name") == TARGET_BOOK).select("tfidf").collect()

if not target_row:
    print(f"Error: {TARGET_BOOK} not found in the loaded data!")
    spark.stop()
    exit()

# Broadcast the target vector to all cores
target_vec = target_row[0]['tfidf'].toArray()
bc_target_vec = spark.sparkContext.broadcast(target_vec)

def calculate_similarity(row_vec):
    v1 = bc_target_vec.value
    v2 = row_vec.toArray()
    dot = float(np.dot(v1, v2))
    norm = np.linalg.norm(v1) * np.linalg.norm(v2)
    return dot / norm if norm != 0 else 0.0

# Calculate similarity for the fetched books
similarity_rdd = processed_df.filter(col("file_name") != TARGET_BOOK).rdd.map(lambda row: (
    row['file_name'], 
    calculate_similarity(row['tfidf'])
))

# Get results

top_results = similarity_rdd.takeOrdered(NUM_BOOKS_TO_COMPARE, key=lambda x: -x[1])

print("\n" + "="*50)
print(f"SIMILARITY SCORES TO {TARGET_BOOK}")
print("="*50)
for name, score in top_results:
    print(f"Book: {name} | Score: {score:.4f}")

spark.stop()