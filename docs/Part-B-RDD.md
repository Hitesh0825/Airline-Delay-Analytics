# Part B — RDD Fundamentals

## 1. What is an RDD?

RDD stands for Resilient Distributed Dataset.

It is Spark's fundamental distributed data structure.

An RDD is:

- Resilient — it can recover from failures using lineage.
- Distributed — data is distributed across partitions.
- Dataset — it represents a collection of data.

---

## 2. Creating an RDD

FlightPulse creates an RDD using:

```scala
val numbers = spark.sparkContext.parallelize(1 to 10)
