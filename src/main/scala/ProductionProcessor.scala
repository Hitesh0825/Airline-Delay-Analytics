import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.LongAccumulator

object ProductionProcessor {

  def main(args: Array[String]): Unit = {

    println()
    println("=" * 70)
    println(" FLIGHTPULSE - PRODUCTION & PERFORMANCE")
    println("=" * 70)

    val spark = SparkSession.builder()
      .appName("FlightPulse-Production-Analytics")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ------------------------------------------------------------
    // 1. READ DATA
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("1. READING FLIGHT DATA")
    println("=" * 70)

    val flights = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/input/flights.csv")

    println(s"Total flight records: ${flights.count()}")

    // ------------------------------------------------------------
    // 2. CACHE
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("2. CACHE DEMONSTRATION")
    println("=" * 70)

    val cachedFlights = flights
      .filter($"cancelled" === 0)
      .cache()

    val cachedCount = cachedFlights.count()

    println(s"Non-cancelled flights: $cachedCount")
    println("DataFrame storage level: MEMORY_ONLY")

    // ------------------------------------------------------------
    // 3. PERSIST
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("3. PERSIST DEMONSTRATION")
    println("=" * 70)

    val persistedFlights = flights
      .filter($"arrivalDelay" > 30)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val delayedCount = persistedFlights.count()

    println(s"Flights delayed by more than 30 minutes: $delayedCount")
    println("Storage level: MEMORY_AND_DISK")

    // ------------------------------------------------------------
    // 4. BROADCAST VARIABLE
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("4. BROADCAST VARIABLE")
    println("=" * 70)

    val airlineMap = Map(
      "AA" -> "American Airlines",
      "AS" -> "Alaska Airlines",
      "B6" -> "JetBlue Airways",
      "DL" -> "Delta Air Lines",
      "F9" -> "Frontier Airlines",
      "NK" -> "Spirit Airlines",
      "UA" -> "United Airlines",
      "WN" -> "Southwest Airlines"
    )

    val broadcastAirlines: Broadcast[Map[String, String]] =
      spark.sparkContext.broadcast(airlineMap)

    val airlineDetails = flights
      .select($"airline")
      .distinct()
      .as[String]
      .map { code =>
        val name = broadcastAirlines.value.getOrElse(code, "Unknown")
        (code, name)
      }
      .toDF("airline", "airlineName")

    airlineDetails.show(false)

    println("Broadcast variable successfully used.")
    println("Purpose: efficiently distribute small lookup data to executors.")

    // ------------------------------------------------------------
    // 5. ACCUMULATOR
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("5. ACCUMULATOR")
    println("=" * 70)

    val severeDelayAccumulator: LongAccumulator =
      spark.sparkContext.longAccumulator("SevereDelayFlights")

    flights.select($"arrivalDelay")
      .as[Double]
      .foreach { delay =>
        if (delay > 60) {
          severeDelayAccumulator.add(1)
        }
      }

    println(
      s"Flights with arrival delay > 60 minutes: ${severeDelayAccumulator.value}"
    )

    println("Accumulator successfully demonstrated.")
    println("Purpose: collect counters from executor-side tasks.")

    // ------------------------------------------------------------
    // 6. PARTITION INFORMATION
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("6. PARTITION INFORMATION")
    println("=" * 70)

    val flightRDD = flights.rdd

    println(s"Original partitions: ${flightRDD.getNumPartitions}")

    val repartitioned = flightRDD.repartition(8)

    println(
      s"Partitions after repartition(8): ${repartitioned.getNumPartitions}"
    )

    val coalesced = repartitioned.coalesce(4)

    println(
      s"Partitions after coalesce(4): ${coalesced.getNumPartitions}"
    )

    // ------------------------------------------------------------
    // 7. PRODUCTION OUTPUT LAYOUT
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("7. PARTITIONED OUTPUT LAYOUT")
    println("=" * 70)

    val productionOutput =
      flights
        .withColumn("year", col("year").cast("int"))
        .withColumn("month", col("month").cast("int"))

    productionOutput
      .repartition($"year", $"month")
      .write
      .mode("overwrite")
      .partitionBy("year", "month")
      .parquet("data/output/production_flights")

    println("Production Parquet output written successfully.")
    println("Layout:")
    println("data/output/production_flights/")
    println("  year=2015/")
    println("    month=1/")
    println("    month=2/")
    println("    ...")
    println("    month=12/")

    // ------------------------------------------------------------
    // 8. SUMMARY OUTPUT
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("8. PRODUCTION SUMMARY")
    println("=" * 70)

    val summary: DataFrame =
      flights
        .groupBy($"airline")
        .agg(
          count("*").alias("totalFlights"),
          round(avg($"departureDelay"), 2).alias("avgDepartureDelay"),
          round(avg($"arrivalDelay"), 2).alias("avgArrivalDelay"),
          sum(when($"arrivalDelay" > 15, 1).otherwise(0))
            .alias("delayedFlights"),
          sum($"cancelled").alias("cancelledFlights")
        )
        .withColumn(
          "delayRate",
          round($"delayedFlights" / $"totalFlights" * 100, 2)
        )
        .orderBy(desc("delayRate"))

    summary.show(false)

    summary.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output/production_airline_summary")

    println("Airline production summary written.")

    // ------------------------------------------------------------
    // 9. CACHE CLEANUP
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("9. RESOURCE CLEANUP")
    println("=" * 70)

    cachedFlights.unpersist()
    persistedFlights.unpersist()

    broadcastAirlines.destroy()

    println("Cached DataFrame released.")
    println("Persisted DataFrame released.")
    println("Broadcast variable destroyed.")

    // ------------------------------------------------------------
    // 10. YARN DEPLOYMENT INFORMATION
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("10. YARN DEPLOYMENT")
    println("=" * 70)

    println("Example production command:")
    println(
      "spark-submit --master yarn --deploy-mode cluster " +
      "--class ProductionProcessor spark-mini-project.jar"
    )

    println()
    println("YARN responsibilities:")
    println("• ResourceManager manages cluster resources")
    println("• ApplicationMaster manages the Spark application")
    println("• NodeManagers run containers on worker nodes")
    println("• Executors perform distributed computation")

    // ------------------------------------------------------------
    // 11. MONITORING
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("11. SPARK MONITORING")
    println("=" * 70)

    println("Spark UI: http://localhost:4040")
    println()
    println("Important Spark UI sections:")
    println("• Jobs")
    println("• Stages")
    println("• Storage")
    println("• Environment")
    println("• Executors")
    println("• SQL")

    // ------------------------------------------------------------
    // FINAL
    // ------------------------------------------------------------

    println()
    println("=" * 70)
    println("PART F COMPLETED SUCCESSFULLY")
    println("=" * 70)

    println("Production concepts demonstrated:")
    println("1. cache()")
    println("2. persist()")
    println("3. broadcast variables")
    println("4. accumulators")
    println("5. repartition()")
    println("6. coalesce()")
    println("7. partitioned Parquet output")
    println("8. production summary")
    println("9. resource cleanup")
    println("10. YARN deployment")
    println("11. Spark monitoring")

    println("=" * 70)
    println("FLIGHTPULSE PRODUCTION DEMO FINISHED")
    println("=" * 70)

    spark.stop()
  }
}
