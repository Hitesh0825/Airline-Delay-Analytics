# Part C — Spark Performance and Partitioning

## 1. Why Partitioning Matters

Spark processes data in parallel using partitions.

Each partition can be processed by a Spark task.

More suitable partitions can improve parallelism, while too many or too few partitions can reduce performance.

---

## 2. Checking Partitions

FlightPulse checks the number of partitions using:

```scala
rdd.getNumPartitions
