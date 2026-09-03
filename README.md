# FlightPulse — Airline Delay Analytics using Apache Spark

## 📌 Project Overview

**FlightPulse** is an Apache Spark-based airline delay analytics project that analyzes flight departure and arrival events to generate insights about **airlines, airports, routes, delays, cancellations, and delay risk**.

The project demonstrates core Spark concepts including **Scala, RDDs, Spark SQL, DataFrames, Datasets, joins, aggregations, window functions, partitioning, caching, broadcasting, accumulators, and DStreams**.

---

## 🎯 Problem Statement

Analyze airline flight events and calculate:

* Airline/carrier delay statistics
* Airport departure and arrival statistics
* Route-level delay statistics
* Monthly flight statistics
* Delay classifications
* Airline rankings
* Route rankings
* Delay Risk Score
* Cancellation and diversion statistics

---

## 🛠️ Technologies Used

* **Scala 2.12.18**
* **Apache Spark 3.5.0**
* **Spark Core**
* **Spark SQL**
* **Spark Streaming**
* **sbt**
* **Java 17**
* **Python**
* **CSV**
* **Parquet**
* **Git & GitHub**
* **Ubuntu / WSL2**

---

## 📂 Project Structure

```text
spark-mini-project/
│
├── src/
│   └── main/
│       └── scala/
│           ├── Models.scala
│           ├── Utils.scala
│           ├── BatchProcessor.scala
│           ├── StreamingProcessor.scala
│           └── ProductionProcessor.scala
│
├── data/
│   ├── input/
│   │   └── flights.csv
│   │
│   ├── reference/
│   │   ├── airlines.csv
│   │   └── airports.csv
│   │
│   └── output/
│
├── docs/
│   ├── Part-A-Scala.md
│   ├── Part-B-RDD.md
│   ├── Part-C-Performance.md
│   ├── Part-D-SparkSQL.md
│   ├── Part-E-Streaming.md
│   └── Part-F-Production.md
│
├── scripts/
│   └── generate_data.py
│
├── sql/
│   └── analytics.sql
│
├── build.sbt
├── README.md
└── .gitignore
```

---

## 📊 Dataset

The project uses a deterministic synthetic airline dataset inspired by the structure of the **U.S. Bureau of Transportation Statistics (BTS) On-Time Performance data**.

The dataset contains **50,000 flight records**.

Each flight contains information such as:

* Year
* Month
* Day
* Day of week
* Airline
* Flight number
* Origin airport
* Destination airport
* Departure delay
* Arrival delay
* Cancellation
* Diversion

The dataset is generated using a fixed random seed so that the project can be reproduced consistently.

---

## 🔎 Analytics Performed

### 1. Overall Flight Statistics

The project calculates:

* Total flights
* Average departure delay
* Average arrival delay
* Total cancelled flights
* Total diverted flights

### 2. Airline Statistics

For every airline:

* Total flights
* Average arrival delay
* Delay rate
* Cancellation rate
* Airline ranking

### 3. Airport Statistics

For every airport:

* Departure flight count
* Arrival flight count
* Average departure delay
* Average arrival delay

Airport reference information is joined using Spark joins.

### 4. Route Statistics

For each origin-destination route:

* Number of flights
* Average arrival delay
* Average departure delay
* Route ranking

### 5. Monthly Statistics

Flight performance is analyzed month by month.

---

## 🚦 Delay Classification

Spark SQL `CASE` expressions are used to classify flight delays:

| Delay         | Classification |
| ------------- | -------------- |
| ≤ 0 minutes   | ON_TIME        |
| 1–15 minutes  | MINOR          |
| 16–60 minutes | MODERATE       |
| > 60 minutes  | SEVERE         |

---

## 📈 Delay Risk Score

FlightPulse introduces a custom **Delay Risk Score from 0–100**.

The score combines multiple operational factors:

* Delay rate
* Average delay severity
* Cancellation rate
* Late arrival rate

The resulting score is classified into risk levels:

* **LOW**
* **MODERATE**
* **HIGH**
* **CRITICAL**

This provides a simple way to identify airlines with higher operational delay risk.

---

## 🧠 Spark Concepts Demonstrated

### Part A — Scala

* Collections
* Vector
* `val`
* `lazy val`
* For-comprehension
* `yield`
* Traits
* Case classes

### Part B — RDD

* RDD creation
* `map`
* `filter`
* `flatMap`
* Pair RDD
* `reduceByKey`
* Transformations
* Actions
* Lineage
* Fault recovery

### Part C — Performance

* Partitions
* Narrow transformations
* Wide transformations
* Shuffle
* `repartition`
* `coalesce`
* Partition inspection

### Part D — Spark SQL

* DataFrames
* Datasets
* Spark SQL
* Temporary views
* Catalog
* UDF
* `CASE`
* Aggregations
* Joins
* Broadcast joins
* Window functions

### Part E — Spark Streaming

* DStreams
* Micro-batches
* Batch interval
* Stateless processing
* Stateful processing
* `updateStateByKey`
* Sliding windows
* Checkpointing

### Part F — Production

* `cache()`
* `persist()`
* Broadcast variables
* Accumulators
* Partitioned Parquet output
* `repartition`
* `coalesce`
* YARN deployment
* Spark UI monitoring
* Output layout

---

## ⚡ Performance Optimization

The project demonstrates several Spark optimization techniques:

### Cache / Persist

Frequently reused datasets are cached or persisted.

```scala
flights.cache()
```

```scala
flights.persist(StorageLevel.MEMORY_AND_DISK)
```

### Broadcast Join

Small reference datasets such as airline information are broadcast to reduce shuffle during joins.

### Repartition

Used when data needs to be redistributed across partitions.

```scala
df.repartition(8)
```

### Coalesce

Used to reduce the number of partitions with less shuffle.

```scala
df.coalesce(4)
```

### Partitioned Output

Production data is written using year and month partitions:

```text
data/output/production_flights/
└── year=2015/
    ├── month=1/
    ├── month=2/
    ├── ...
    └── month=12/
```

---

## 📦 Output Files

The batch analytics generate:

```text
data/output/
├── airline_statistics/
├── airport_statistics/
├── route_statistics/
├── delay_risk/
├── monthly_statistics/
└── summary/
```

Production processing additionally generates:

```text
production_flights/
production_airline_summary/
```

---

## ▶️ How to Run

### 1. Generate Dataset

```bash
python3 scripts/generate_data.py
```

### 2. Compile Project

```bash
sbt clean compile
```

### 3. Run Batch Analytics

```bash
sbt "runMain BatchProcessor"
```

### 4. Run Streaming Analytics

```bash
sbt "runMain StreamingProcessor"
```

### 5. Run Production Processing

```bash
sbt "runMain ProductionProcessor"
```

---

## 🗃️ SQL Analytics

SQL queries are available in:

```text
sql/analytics.sql
```

The file contains queries for:

1. Overall flight summary
2. Airline statistics
3. Delay classification
4. Airport statistics
5. Route statistics
6. Monthly statistics
7. Top routes
8. Airline ranking
9. Route ranking

---

## 📚 Documentation

Detailed documentation for each assessment section is available in the `docs` directory:

* `Part-A-Scala.md`
* `Part-B-RDD.md`
* `Part-C-Performance.md`
* `Part-D-SparkSQL.md`
* `Part-E-Streaming.md`
* `Part-F-Production.md`

---

## 🌐 Reference Dataset

The project structure is inspired by the U.S. Bureau of Transportation Statistics airline on-time performance data.

Official source:

https://www.transtats.bts.gov/ontime/

---

## 💡 Learning Outcomes

Through this project, the following Apache Spark concepts were practiced:

* Building Spark applications with Scala
* Working with RDDs
* DataFrame and Dataset APIs
* Spark SQL analytics
* Aggregation and grouping
* Window functions
* Joins and broadcast joins
* UDFs
* Partition management
* Shuffle optimization
* Caching and persistence
* Accumulators
* Spark Streaming
* Stateful and window-based processing
* Parquet data layout
* Production-oriented Spark design
* YARN deployment concepts

---

## ✅ Project Status

**FlightPulse — Airline Delay Analytics**

All assessment parts have been implemented:

* ✅ Part A — Scala
* ✅ Part B — RDD
* ✅ Part C — Performance
* ✅ Part D — Spark SQL
* ✅ Part E — Streaming
* ✅ Part F — Production
* ✅ Documentation
* ✅ Git repository
* ✅ GitHub repository

---

## 👨‍💻 Author

**Hitesh Sharma**

B.Tech — Data Science & Artificial Intelligence

