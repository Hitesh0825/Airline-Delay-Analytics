import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.rdd.RDD

object BatchProcessor {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // SPARK SESSION
    // ============================================================

    val spark: SparkSession =
      SparkUtils.createSparkSession(
        "FlightPulse-Airline-Delay-Analytics"
      )

    val sc = spark.sparkContext

    spark.sparkContext.setLogLevel("WARN")

    println()
    println("=" * 70)
    println("FLIGHTPULSE - AIRLINE DELAY ANALYTICS")
    println("=" * 70)

    println(s"Spark Version: ${spark.version}")
    println(s"Application Name: ${spark.sparkContext.appName}")

    // ============================================================
    // PART A - SCALA FUNDAMENTALS
    // ============================================================

    println()
    println("=" * 70)
    println("PART A - SCALA FUNDAMENTALS")
    println("=" * 70)

    val airlines = List("AA", "DL", "UA", "WN", "B6")

    val airlineVector =
      Vector("AA", "DL", "UA", "WN", "B6")

    val airlineNames = Map(
      "AA" -> "American Airlines",
      "DL" -> "Delta Air Lines",
      "UA" -> "United Airlines",
      "WN" -> "Southwest Airlines",
      "B6" -> "JetBlue Airways"
    )

    val airports = Set(
      "ATL",
      "DFW",
      "DEN",
      "ORD",
      "LAX"
    )

    val formattedAirlines =
      for {
        airline <- airlines
      } yield airlineNames.getOrElse(
        airline,
        "Unknown Airline"
      )

    lazy val projectDescription =
      "FlightPulse - Airline Delay Analytics using Apache Spark"

    println(s"Airlines List: $airlines")
    println(s"Airline Vector: $airlineVector")
    println(s"Airline Map: $airlineNames")
    println(s"Airport Set: $airports")
    println(s"For-Comprehension + Yield: $formattedAirlines")
    println(s"Lazy Value: $projectDescription")

    val sampleDelays =
      List(-5.0, 0.0, 10.0, 35.0, 90.0)

    println()
    println("Delay Classification:")

    sampleDelays.foreach { delay =>
      println(
        f"Delay: $delay%6.1f minutes -> ${SparkUtils.classifyDelay(delay)}"
      )
    }

    println()
    println("Part A completed successfully.")

    // ============================================================
    // PART B - RDD FUNDAMENTALS
    // ============================================================

    println()
    println("=" * 70)
    println("PART B - RDD FUNDAMENTALS")
    println("=" * 70)

    val flightData =
      Seq(
        ("AA", "ATL", "DFW", 25.0),
        ("AA", "ATL", "LAX", 65.0),
        ("DL", "JFK", "ATL", 10.0),
        ("DL", "ATL", "ORD", 45.0),
        ("UA", "ORD", "DEN", 80.0),
        ("UA", "DEN", "LAX", -5.0),
        ("WN", "DAL", "HOU", 30.0),
        ("WN", "LAX", "LAS", 70.0),
        ("B6", "JFK", "LAX", 15.0),
        ("B6", "BOS", "JFK", 95.0)
      )

    val flightRDD:
      RDD[(String, String, String, Double)] =
      sc.parallelize(flightData, 4)

    println(s"RDD Record Count: ${flightRDD.count()}")
    println(s"RDD Partitions: ${flightRDD.getNumPartitions}")

    val routeRDD =
      flightRDD.map {
        case (
              carrier,
              origin,
              destination,
              delay
            ) =>
          (s"$origin-$destination", delay)
      }

    println()
    println("MAP TRANSFORMATION:")
    routeRDD.collect().foreach(println)

    val delayedFlightsRDD =
      flightRDD.filter {
        case (_, _, _, delay) =>
          delay > 30
      }

    println()
    println(
      s"Flights with delay > 30 minutes: ${delayedFlightsRDD.count()}"
    )

    val airportRDD =
      flightRDD.flatMap {
        case (
              _,
              origin,
              destination,
              _
            ) =>
          Seq(origin, destination)
      }

    println()
    println("FLATMAP TRANSFORMATION:")
    airportRDD.collect().foreach(println)

    val carrierDelayCountRDD =
      flightRDD
        .filter {
          case (_, _, _, delay) =>
            delay > 0
        }
        .map {
          case (carrier, _, _, _) =>
            (carrier, 1)
        }
        .reduceByKey(_ + _)

    println()
    println("PAIR RDD - DELAYED FLIGHTS BY CARRIER:")

    carrierDelayCountRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (carrier, count) =>
          println(s"$carrier -> $count")
      }

    val carrierTotalDelayRDD =
      flightRDD
        .map {
          case (
                carrier,
                _,
                _,
                delay
              ) =>
            (carrier, delay)
        }
        .reduceByKey(_ + _)

    println()
    println("TOTAL DELAY MINUTES BY CARRIER:")

    carrierTotalDelayRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (
              carrier,
              delay
            ) =>
          println(
            f"$carrier -> $delay%.1f minutes"
          )
      }

    val totalFlights =
      flightRDD.count()

    val totalDelay =
      flightRDD
        .map {
          case (
                _,
                _,
                _,
                delay
              ) =>
            delay
        }
        .reduce(_ + _)

    println()
    println("RDD ACTIONS:")
    println(s"Total Flights: $totalFlights")
    println(f"Total Delay: $totalDelay%.1f minutes")

    println()
    println("RDD LINEAGE:")
    println(carrierDelayCountRDD.toDebugString)

    println()
    println("Part B completed successfully.")

    // ============================================================
    // PART C - PERFORMANCE & PARTITIONS
    // ============================================================

    println()
    println("=" * 70)
    println("PART C - SPARK PERFORMANCE & PARTITIONS")
    println("=" * 70)

    val initialPartitions =
      flightRDD.getNumPartitions

    println()
    println(
      s"Initial RDD Partitions: $initialPartitions"
    )

    // Narrow transformation
    val narrowRDD =
      flightRDD
        .filter {
          case (_, _, _, delay) =>
            delay >= 0
        }
        .map {
          case (
                carrier,
                origin,
                destination,
                delay
              ) =>
            (
              carrier,
              s"$origin-$destination",
              delay
            )
        }

    println()
    println("NARROW TRANSFORMATION:")
    println("Operations: filter + map")
    println(
      s"Narrow RDD Partitions: ${narrowRDD.getNumPartitions}"
    )

    // Wide transformation
    val wideRDD =
      flightRDD
        .map {
          case (
                carrier,
                _,
                _,
                delay
              ) =>
            (carrier, delay)
        }
        .reduceByKey(_ + _)

    println()
    println("WIDE TRANSFORMATION:")
    println("Operations: map + reduceByKey")
    println(
      s"Wide RDD Partitions: ${wideRDD.getNumPartitions}"
    )

    // Repartition
    val repartitionedRDD =
      flightRDD.repartition(8)

    println()
    println("REPARTITION:")
    println(
      s"Before: ${flightRDD.getNumPartitions}"
    )
    println(
      s"After: ${repartitionedRDD.getNumPartitions}"
    )
    println("Repartition causes a full shuffle.")

    // Coalesce
    val coalescedRDD =
      repartitionedRDD.coalesce(4)

    println()
    println("COALESCE:")
    println(
      s"Before: ${repartitionedRDD.getNumPartitions}"
    )
    println(
      s"After: ${coalescedRDD.getNumPartitions}"
    )
    println("Coalesce reduces partitions.")

    println()
    println("PARTITION COMPARISON:")
    println(
      s"Original RDD      : ${flightRDD.getNumPartitions}"
    )
    println(
      s"Narrow RDD        : ${narrowRDD.getNumPartitions}"
    )
    println(
      s"Wide RDD          : ${wideRDD.getNumPartitions}"
    )
    println(
      s"Repartitioned RDD : ${repartitionedRDD.getNumPartitions}"
    )
    println(
      s"Coalesced RDD     : ${coalescedRDD.getNumPartitions}"
    )

    println()
    println(
      s"Repartitioned records: ${repartitionedRDD.count()}"
    )
    println(
      s"Coalesced records: ${coalescedRDD.count()}"
    )

    val partitionInfo =
      flightRDD.mapPartitionsWithIndex {
        case (
              partitionId,
              records
            ) =>

          val rows =
            records.toList

          Iterator(
            s"Partition $partitionId contains ${rows.size} records"
          )
      }

    println()
    println("PARTITION LEVEL INFORMATION:")
    partitionInfo.collect().foreach(println)

    println()
    println("Part C completed successfully.")

    // ============================================================
    // PART D - SPARK SQL & DATAFRAME ANALYTICS
    // ============================================================

    println()
    println("=" * 70)
    println("PART D - SPARK SQL & DATAFRAME ANALYTICS")
    println("=" * 70)

    import spark.implicits._

    // ------------------------------------------------------------
    // Read flight data
    // ------------------------------------------------------------

    val flightPath =
      "data/input/flights.csv"

    val flightsDF =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(flightPath)

    println()
    println("FLIGHT DATAFRAME SCHEMA:")
    flightsDF.printSchema()

    val flightCount =
      flightsDF.count()

    println()
    println(
      s"Flight DataFrame Count: $flightCount"
    )

    // ------------------------------------------------------------
    // Dataset
    // ------------------------------------------------------------

    val flightsDS: Dataset[Flight] =
      flightsDF
        .select(
          $"year",
          $"month",
          $"day",
          $"dayOfWeek",
          $"airline",
          $"flightNumber",
          $"origin",
          $"destination",
          $"departureDelay",
          $"arrivalDelay",
          $"cancelled",
          $"diverted"
        )
        .as[Flight]

    println()
    println(
      s"Typed Dataset Count: ${flightsDS.count()}"
    )

    // ------------------------------------------------------------
    // Spark Catalog
    // ------------------------------------------------------------

    flightsDF.createOrReplaceTempView("flights")

    println()
    println("SPARK CATALOG:")

    spark.catalog
      .listTables()
      .show(false)

    // ------------------------------------------------------------
    // Overall SQL summary
    // ------------------------------------------------------------

    val sqlSummary =
      spark.sql(
        """
          |SELECT
          |  COUNT(*) AS total_flights,
          |  ROUND(AVG(departureDelay), 2)
          |      AS avg_departure_delay,
          |  ROUND(AVG(arrivalDelay), 2)
          |      AS avg_arrival_delay,
          |  SUM(cancelled) AS cancelled_flights,
          |  SUM(diverted) AS diverted_flights
          |FROM flights
          |""".stripMargin
      )

    println()
    println("OVERALL FLIGHT SUMMARY:")

    sqlSummary.show(false)

    // ------------------------------------------------------------
    // CASE expression
    // ------------------------------------------------------------

    val classifiedFlights =
      flightsDF.withColumn(
        "delay_category",
        when(
          $"arrivalDelay" <= 0,
          "ON_TIME"
        )
          .when(
            $"arrivalDelay" <= 15,
            "MINOR"
          )
          .when(
            $"arrivalDelay" <= 60,
            "MODERATE"
          )
          .otherwise(
            "SEVERE"
          )
      )

    println()
    println("CASE - DELAY CLASSIFICATION:")

    classifiedFlights
      .groupBy("delay_category")
      .count()
      .orderBy(desc("count"))
      .show(false)

    // ------------------------------------------------------------
    // UDF
    // ------------------------------------------------------------

    val delayRiskUDF =
      udf {
        (
            delay: Double,
            cancelled: Int,
            diverted: Int
        ) =>

          if (cancelled == 1) {
            "CANCELLED"
          } else if (diverted == 1) {
            "DIVERTED"
          } else if (delay <= 15) {
            "LOW"
          } else if (delay <= 60) {
            "MEDIUM"
          } else {
            "HIGH"
          }
      }

    val enrichedFlights =
      classifiedFlights.withColumn(
        "delay_risk",
        delayRiskUDF(
          $"arrivalDelay",
          $"cancelled",
          $"diverted"
        )
      )

    println()
    println("UDF - DELAY RISK:")

    enrichedFlights
      .select(
        "airline",
        "origin",
        "destination",
        "arrivalDelay",
        "delay_category",
        "delay_risk"
      )
      .show(10, false)

    // ------------------------------------------------------------
    // Load reference data
    // ------------------------------------------------------------

    val airlinesDF =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(
          "data/reference/airlines.csv"
        )

    val airportsDF =
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(
          "data/reference/airports.csv"
        )

    // ------------------------------------------------------------
    // Airline Analytics + JOIN
    // ------------------------------------------------------------

    val carrierStats =
      enrichedFlights
        .join(
          broadcast(airlinesDF),
          enrichedFlights("airline") ===
            airlinesDF("code"),
          "left"
        )
        .groupBy(
          airlinesDF("code"),
          airlinesDF("name")
        )
        .agg(
          count("*").alias(
            "total_flights"
          ),
          round(
            avg("departureDelay"),
            2
          ).alias(
            "avg_departure_delay"
          ),
          round(
            avg("arrivalDelay"),
            2
          ).alias(
            "avg_arrival_delay"
          ),
          sum(
            when(
              $"arrivalDelay" > 15,
              1
            ).otherwise(0)
          ).alias(
            "delayed_flights"
          ),
          sum(
            when(
              $"cancelled" === 1,
              1
            ).otherwise(0)
          ).alias(
            "cancelled_flights"
          ),
          sum(
            when(
              $"diverted" === 1,
              1
            ).otherwise(0)
          ).alias(
            "diverted_flights"
          )
        )
        .withColumn(
          "delay_rate_pct",
          round(
            $"delayed_flights" /
              $"total_flights" * 100,
            2
          )
        )
        .withColumn(
          "cancellation_rate_pct",
          round(
            $"cancelled_flights" /
              $"total_flights" * 100,
            2
          )
        )
        .orderBy(
          desc("delay_rate_pct")
        )

    println()
    println("AIRLINE DELAY STATISTICS:")

    carrierStats.show(false)

    // ------------------------------------------------------------
    // Airport Departure Analytics
    // ------------------------------------------------------------

    val originAirportStats =
      enrichedFlights
        .groupBy("origin")
        .agg(
          count("*").alias(
            "departures"
          ),
          round(
            avg("departureDelay"),
            2
          ).alias(
            "avg_departure_delay"
          ),
          sum(
            when(
              $"departureDelay" > 15,
              1
            ).otherwise(0)
          ).alias(
            "delayed_departures"
          )
        )
        .withColumn(
          "delay_rate_pct",
          round(
            $"delayed_departures" /
              $"departures" * 100,
            2
          )
        )

    println()
    println("DEPARTURE AIRPORT STATISTICS:")

    originAirportStats
      .orderBy(
        desc("delay_rate_pct")
      )
      .show(
        10,
        false
      )

    // ------------------------------------------------------------
    // Airport Destination Analytics
    // ------------------------------------------------------------

    val destinationAirportStats =
      enrichedFlights
        .groupBy("destination")
        .agg(
          count("*").alias(
            "arrivals"
          ),
          round(
            avg("arrivalDelay"),
            2
          ).alias(
            "avg_arrival_delay"
          ),
          sum(
            when(
              $"arrivalDelay" > 15,
              1
            ).otherwise(0)
          ).alias(
            "delayed_arrivals"
          )
        )
        .withColumn(
          "delay_rate_pct",
          round(
            $"delayed_arrivals" /
              $"arrivals" * 100,
            2
          )
        )

    println()
    println("ARRIVAL AIRPORT STATISTICS:")

    destinationAirportStats
      .orderBy(
        desc("delay_rate_pct")
      )
      .show(
        10,
        false
      )

    // ------------------------------------------------------------
    // Airport JOIN with reference data
    // ------------------------------------------------------------

    val airportDetails =
      originAirportStats
        .join(
          broadcast(airportsDF),
          originAirportStats("origin") ===
            airportsDF("code"),
          "left"
        )
        .select(
          airportsDF("code"),
          airportsDF("name"),
          airportsDF("city"),
          airportsDF("state"),
          $"departures",
          $"avg_departure_delay",
          $"delayed_departures",
          $"delay_rate_pct"
        )

    println()
    println("AIRPORT DETAILS USING JOIN:")

    airportDetails
      .orderBy(
        desc("delay_rate_pct")
      )
      .show(
        10,
        false
      )

    // ------------------------------------------------------------
    // Route Analytics
    // ------------------------------------------------------------

    val routeStats =
      enrichedFlights
        .withColumn(
          "route",
          concat_ws(
            "-",
            $"origin",
            $"destination"
          )
        )
        .groupBy("route")
        .agg(
          count("*").alias(
            "total_flights"
          ),
          round(
            avg("departureDelay"),
            2
          ).alias(
            "avg_departure_delay"
          ),
          round(
            avg("arrivalDelay"),
            2
          ).alias(
            "avg_arrival_delay"
          ),
          sum(
            when(
              $"arrivalDelay" > 15,
              1
            ).otherwise(0)
          ).alias(
            "delayed_flights"
          )
        )
        .withColumn(
          "delay_rate_pct",
          round(
            $"delayed_flights" /
              $"total_flights" * 100,
            2
          )
        )
        .orderBy(
          desc("avg_arrival_delay")
        )

    println()
    println(
      "TOP ROUTES BY AVERAGE ARRIVAL DELAY:"
    )

    routeStats
      .show(
        10,
        false
      )

    // ------------------------------------------------------------
    // Window Function - Airline Ranking
    // ------------------------------------------------------------

    val airlineWindow =
      Window
        .orderBy(
          desc("avg_arrival_delay")
        )

    val rankedAirlines =
      carrierStats.withColumn(
        "delay_rank",
        dense_rank().over(
          airlineWindow
        )
      )

    println()
    println(
      "AIRLINE RANKING USING WINDOW FUNCTION:"
    )

    rankedAirlines
      .select(
        "code",
        "name",
        "avg_arrival_delay",
        "delay_rate_pct",
        "delay_rank"
      )
      .show(false)

    // ------------------------------------------------------------
    // Window Function - Route Ranking by Origin
    // ------------------------------------------------------------

    val routeWindow =
      Window
        .partitionBy("origin")
        .orderBy(
          desc("avg_arrival_delay")
        )

    val rankedRoutes =
      enrichedFlights
        .withColumn(
          "route",
          concat_ws(
            "-",
            $"origin",
            $"destination"
          )
        )
        .groupBy(
          "origin",
          "destination",
          "route"
        )
        .agg(
          round(
            avg("arrivalDelay"),
            2
          ).alias(
            "avg_arrival_delay"
          ),
          count("*").alias(
            "total_flights"
          )
        )
        .withColumn(
          "route_rank",
          dense_rank().over(
            routeWindow
          )
        )

    println()
    println(
      "TOP ROUTES WITHIN EACH ORIGIN:"
    )

    rankedRoutes
      .filter(
        $"route_rank" <= 3
      )
      .orderBy(
        "origin",
        "route_rank"
      )
      .show(
        20,
        false
      )

    // ------------------------------------------------------------
    // Unique Delay Risk Score
    // ------------------------------------------------------------

    val riskScores =
      carrierStats
        .withColumn(
          "delay_severity_score",
          least(
            lit(100.0),
            greatest(
              lit(0.0),
              $"avg_arrival_delay" / 2.0
            )
          )
        )
        .withColumn(
          "delay_rate_score",
          least(
            lit(100.0),
            $"delay_rate_pct"
          )
        )
        .withColumn(
          "cancellation_score",
          least(
            lit(100.0),
            $"cancellation_rate_pct" * 5.0
          )
        )
        .withColumn(
          "delay_risk_score",
          round(
            $"delay_rate_score" * 0.50 +
              $"delay_severity_score" * 0.35 +
              $"cancellation_score" * 0.15,
            2
          )
        )
        .withColumn(
          "risk_level",
          when(
            $"delay_risk_score" < 25,
            "LOW"
          )
            .when(
              $"delay_risk_score" < 50,
              "MODERATE"
            )
            .when(
              $"delay_risk_score" < 75,
              "HIGH"
            )
            .otherwise(
              "CRITICAL"
            )
        )
        .orderBy(
          desc("delay_risk_score")
        )

    println()
    println(
      "AIRLINE DELAY RISK SCORE:"
    )

    riskScores
      .select(
        "code",
        "name",
        "delay_rate_pct",
        "avg_arrival_delay",
        "cancellation_rate_pct",
        "delay_risk_score",
        "risk_level"
      )
      .show(false)

    // ------------------------------------------------------------
    // Monthly SQL Analytics
    // ------------------------------------------------------------

    val monthlyStats =
      spark.sql(
        """
          |SELECT
          |  month,
          |  airline,
          |  COUNT(*) AS total_flights,
          |  ROUND(
          |    AVG(departureDelay),
          |    2
          |  ) AS avg_departure_delay,
          |  ROUND(
          |    AVG(arrivalDelay),
          |    2
          |  ) AS avg_arrival_delay,
          |  SUM(
          |    CASE
          |      WHEN arrivalDelay > 15
          |      THEN 1
          |      ELSE 0
          |    END
          |  ) AS delayed_flights
          |FROM flights
          |GROUP BY
          |  month,
          |  airline
          |ORDER BY
          |  month,
          |  airline
          |""".stripMargin
      )

    println()
    println(
      "MONTHLY AIRLINE STATISTICS:"
    )

    monthlyStats
      .show(
        20,
        false
      )

    // ============================================================
    // OUTPUT SECTION
    // ============================================================

    println()
    println("=" * 70)
    println("WRITING ANALYTICS OUTPUT")
    println("=" * 70)

    val outputBase =
      "data/output"

    // Airline statistics
    carrierStats
      .write
      .mode("overwrite")
      .parquet(
        s"$outputBase/airline_statistics"
      )

    println(
      "Written: airline_statistics"
    )

    // Airport statistics
    airportDetails
      .write
      .mode("overwrite")
      .parquet(
        s"$outputBase/airport_statistics"
      )

    println(
      "Written: airport_statistics"
    )

    // Route statistics
    routeStats
      .write
      .mode("overwrite")
      .parquet(
        s"$outputBase/route_statistics"
      )

    println(
      "Written: route_statistics"
    )

    // Delay risk
    riskScores
      .write
      .mode("overwrite")
      .parquet(
        s"$outputBase/delay_risk"
      )

    println(
      "Written: delay_risk"
    )

    // Monthly statistics
    monthlyStats
      .write
      .mode("overwrite")
      .parquet(
        s"$outputBase/monthly_statistics"
      )

    println(
      "Written: monthly_statistics"
    )

    // Overall summary as CSV
    sqlSummary
      .coalesce(1)
      .write
      .mode("overwrite")
      .option(
        "header",
        "true"
      )
      .csv(
        s"$outputBase/summary"
      )

    println(
      "Written: summary"
    )

    println()
    println(
      "All Part D outputs written successfully."
    )

    println()
    println(
      "Part D completed successfully."
    )

    // ============================================================
    // FINAL STATUS
    // ============================================================

    println()
    println("=" * 70)
    println(
      "PART A + PART B + PART C + PART D COMPLETED"
    )
    println("=" * 70)

    spark.stop()
  }
}
