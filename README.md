# ML_with_Big_Data
# Big Data Analytics: Hadoop MapReduce & Apache Spark

[cite_start]**Author:** Jyoti Dwivedi (M25CSA010) [cite: 161, 162]
[cite_start]**Course:** ML with Big Data - Assignment 1 [cite: 159, 160]

##  Overview
[cite_start]This repository contains the implementation of an end-to-end Big Data analytical pipeline using **Apache Hadoop** and **Apache Spark**[cite: 164]. [cite_start]The project processes a corpus of over 400 text files from the Project Gutenberg dataset to explore distributed data processing, Natural Language Processing (TF-IDF), and Network Graph Analysis[cite: 164, 165]. 

[cite_start]A major component of this project involved profiling and engineering around severe hardware constraints (1.5 GB available RAM on a WSL2 environment), which required significant optimization to prevent JVM Out-Of-Memory crashes.

##  Tech Stack
* [cite_start]**Frameworks:** Apache Hadoop (HDFS, MapReduce), Apache Spark (PySpark) [cite: 164]
* [cite_start]**Language:** Python, Java [cite: 222]
* [cite_start]**Environment:** Single Node Cluster on Windows Subsystem for Linux (WSL2) [cite: 221]

## Key Implementations

### 1. Apache Hadoop & MapReduce
* [cite_start]**Distributed WordCount:** Implemented the foundational MapReduce paradigm to tokenize and count word frequencies across the Gutenberg corpus[cite: 169, 170].
* [cite_start]**Performance Tuning:** Benchmarked the execution impact of the `mapreduce.input.fileinputformat.split.maxsize` parameter[cite: 476]. [cite_start]Demonstrated the performance trade-off between parallel execution capacity (number of Mappers) and JVM task-management overhead[cite: 485, 487].
* [cite_start]**HDFS Architecture:** Analyzed HDFS namespace management, demonstrating why physical data blocks utilize a replication factor while logical directory metadata does not[cite: 394, 396].

### 2. Apache Spark & PySpark
* [cite_start]**Regex Metadata Extraction:** Built a PySpark pipeline to parse unstructured `.txt` files using Regular Expressions, extracting fields such as `Title`, `Release Date`, `Language`, and `Encoding`[cite: 806, 810]. [cite_start]Calculated aggregates including average title length and yearly release distributions[cite: 633].
* [cite_start]**TF-IDF & Cosine Similarity:** Engineered a distributed NLP pipeline to calculate Term Frequency-Inverse Document Frequency (TF-IDF) vectors[cite: 818, 819]. [cite_start]Computed Cosine Similarity to find highly related texts within a subset of 25 books, intentionally optimizing the workload to bypass $O(N^2)$ Cartesian shuffle crashes on limited RAM[cite: 821, 823, 825].
* [cite_start]**Author Influence Network:** Constructed a directed network graph linking authors who published within a 5-year temporal window of one another[cite: 947, 1021]. Calculated topological centrality:
  * [cite_start]**Top In-Degree (Most Influenced):** Robert Louis Stevenson (584), Thomas Hardy (461)[cite: 949].
  * [cite_start]**Top Out-Degree (Most Influential):** Thomas Hardy (644), Robert Louis Stevenson (504)[cite: 950].
  * [cite_start]*Analyzed scaling limitations, proposing Time Bucketing (Windowing) to eliminate Theta-Join bottlenecks for datasets scaling to millions of books*[cite: 1023, 1024, 1025].

## System Constraints & Optimization
[cite_start]Running distributed cross-corpus algorithms locally triggered `java.lang.OutOfMemoryError` limits and killed the Hadoop execution processes[cite: 824, 829]. [cite_start]To force the jobs to succeed within 1.5 GB of RAM, the algorithms were optimized by bypassing heavy PySpark DataFrames where necessary and carefully bounding the input data subsets[cite: 167, 825].

---
*This repository was submitted as part of the CSL7110 curriculum.*
