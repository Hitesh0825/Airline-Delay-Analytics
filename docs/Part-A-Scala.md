# Part A — Scala Fundamentals

## Objective

This section demonstrates core Scala programming concepts required for the
Airline Delay Analytics project.

## 1. Collections

The project uses Scala collections to represent and process airline,
airport and flight-related information.

Examples include:

- List
- Vector
- Map
- Set

## 2. For-Comprehension

For-comprehensions provide a readable way to iterate over collections and
combine multiple operations.

Example:

```scala
for {
  airline <- airlines
  if airline.code.nonEmpty
} yield airline.name
