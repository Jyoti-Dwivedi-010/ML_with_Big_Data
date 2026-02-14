# ML_with_Big_Data
# Big Data Analytics: Hadoop MapReduce & Apache Spark

**Author:** Jyoti Dwivedi (M25CSA010)
**Course:** ML with Big Data - Assignment 1 

##  Overview
This repository contains the implementation of an end-to-end Big Data analytical pipeline using **Apache Hadoop** and **Apache Spark**. The project processes a corpus of over 400 text files from the Project Gutenberg dataset to explore distributed data processing, Natural Language Processing (TF-IDF), and Network Graph Analysis. 

A major component of this project involved profiling and engineering around severe hardware constraints (1.5 GB available RAM on a WSL2 environment), which required significant optimization to prevent JVM Out-Of-Memory crashes.

##  Tech Stack
**Frameworks:** Apache Hadoop (HDFS, MapReduce), Apache Spark (PySpark) 
**Language:** Python, Java 
**Environment:** Single Node Cluster on Windows Subsystem for Linux (WSL2)

## Key Implementations

### 1. Apache Hadoop & MapReduce
**Distributed WordCount:** Implemented the foundational MapReduce paradigm to tokenize and count word frequencies across the Gutenberg corpus.
**Performance Tuning:** Benchmarked the execution impact of the `mapreduce.input.fileinputformat.split.maxsize` parameter. Demonstrated the performance trade-off between parallel execution capacity (number of Mappers) and JVM task-management overhead
**HDFS Architecture:** Analyzed HDFS namespace management, demonstrating why physical data blocks utilize a replication factor while logical directory metadata does not.

### 2. Apache Spark & PySpark
**Regex Metadata Extraction:** Built a PySpark pipeline to parse unstructured `.txt` files using Regular Expressions, extracting fields such as `Title`, `Release Date`, `Language`, and `Encoding`. Calculated aggregates including average title length and yearly release distributions
**TF-IDF & Cosine Similarity:** Engineered a distributed NLP pipeline to calculate Term Frequency-Inverse Document Frequency (TF-IDF) vectors.Computed Cosine Similarity to find highly related texts within a subset of 25 books, intentionally optimizing the workload to bypass O(N^2) Cartesian shuffle crashes on limited RAM.
**Author Influence Network:** Constructed a directed network graph linking authors who published within a 5-year temporal window of one another. Calculated topological centrality:
  **Top In-Degree (Most Influenced):** Robert Louis Stevenson (584), Thomas Hardy (461).
  **Top Out-Degree (Most Influential):** Thomas Hardy (644), Robert Louis Stevenson (504).
  *Analyzed scaling limitations, proposing Time Bucketing (Windowing) to eliminate Theta-Join bottlenecks for datasets scaling to millions of books*.

## System Constraints & Optimization
]Running distributed cross-corpus algorithms locally triggered `java.lang.OutOfMemoryError` limits and killed the Hadoop execution processes.To force the jobs to succeed within 1.5 GB of RAM, the algorithms were optimized by bypassing heavy PySpark DataFrames where necessary and carefully bounding the input data subsets.

---
*This repository was submitted as part of the CSL7110 curriculum.*
