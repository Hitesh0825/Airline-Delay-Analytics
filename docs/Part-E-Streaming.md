# Part E — Spark Streaming

## 1. Introduction

Spark Streaming allows Spark applications to process incoming data continuously.

FlightPulse demonstrates Spark Streaming using DStreams and micro-batch processing.

The project demonstrates:

- DStreams
- Micro-batch processing
- Batch interval
- Stateless processing
- Stateful processing
- updateStateByKey
- Sliding windows
- Window duration
- Slide duration
- Checkpointing concept

---

## 2. DStreams

DStream stands for Discretized Stream.

A DStream represents a continuous stream of data divided into small batches.

Conceptually:

```text
Continuous Data
      |
      v
+-----+-----+-----+-----+
| B1  | B2  | B3  | B4  |
+-----+-----+-----+-----+
      |
      v
Spark Streaming
