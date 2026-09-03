# Part D — Spark SQL and DataFrame Analytics

## 1. Introduction

Spark SQL is Spark's module for working with structured and semi-structured data.

FlightPulse uses Spark SQL to analyze airline flight delays.

The project demonstrates:

- DataFrames
- Datasets
- Spark SQL
- Spark Catalog
- Temporary views
- CASE expressions
- UDFs
- Aggregations
- Joins
- Broadcast joins
- Window functions
- Partitioned windows
- Monthly analytics
- Parquet output

## 2. Reading CSV Data

FlightPulse reads the flight dataset using:

```scala
val flights = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/input/flights.csv")
