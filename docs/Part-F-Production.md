# Part F — Production and Deployment

## 1. Introduction

Production Spark applications need more than just correct transformations.

They also need:

- Memory management
- Efficient joins
- Partition management
- Fault tolerance
- Monitoring
- Proper output layout
- Cluster deployment
- Resource cleanup

FlightPulse demonstrates these production concepts.

---

## 2. cache()

`cache()` keeps a DataFrame or RDD in memory for reuse.

FlightPulse uses:

```scala
val cachedFlights = flights
  .filter($"cancelled" === 0)
  .cache()
