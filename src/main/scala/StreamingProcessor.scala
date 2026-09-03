import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds

object StreamingProcessor {

  def main(args: Array[String]): Unit = {

    println()
    println("=" * 70)
    println("FLIGHTPULSE - STREAMING DELAY ANALYTICS")
    println("=" * 70)

    // ============================================================
    // PART E - SPARK STREAMING / DSTREAMS
    // ============================================================

    /*
     * We use a local queueStream so that the demonstration
     * is deterministic and does not require Kafka or another
     * external streaming system.
     */

    val conf =
      new SparkConf()
        .setAppName("FlightPulse-Streaming-Analytics")
        .setMaster("local[2]")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.driver.bindAddress", "127.0.0.1")

    // ------------------------------------------------------------
    // Batch Interval
    // ------------------------------------------------------------

    val batchInterval = Seconds(2)

    val streamingContext =
      new StreamingContext(
        conf,
        batchInterval
      )

    streamingContext.sparkContext.setLogLevel("WARN")

    println()
    println(
      s"Batch Interval: ${batchInterval.milliseconds} milliseconds"
    )

    // Stateful operations require a checkpoint directory.
    streamingContext.checkpoint(
      "data/output/streaming_checkpoint"
    )

    // ============================================================
    // SAMPLE STREAMING DATA
    // ============================================================

    val batch1 =
      streamingContext.sparkContext.parallelize(
        Seq(
          "AA,ATL,DFW,25",
          "DL,JFK,ATL,10",
          "UA,ORD,DEN,80",
          "WN,LAX,LAS,70",
          "B6,BOS,JFK,95"
        )
      )

    val batch2 =
      streamingContext.sparkContext.parallelize(
        Seq(
          "AA,ATL,LAX,65",
          "DL,ATL,ORD,45",
          "UA,DEN,LAX,20",
          "WN,DAL,HOU,30",
          "B6,JFK,LAX,15"
        )
      )

    val batch3 =
      streamingContext.sparkContext.parallelize(
        Seq(
          "AA,DFW,LAX,90",
          "DL,ATL,JFK,5",
          "UA,ORD,LAX,55",
          "WN,LAX,PHX,75",
          "B6,BOS,LAX,40"
        )
      )

    val batch4 =
      streamingContext.sparkContext.parallelize(
        Seq(
          "AA,LAX,JFK,35",
          "DL,JFK,ATL,0",
          "UA,DEN,SFO,100",
          "WN,LAS,DEN,25",
          "B6,JFK,BOS,60"
        )
      )

    val inputStream =
      streamingContext.queueStream(
        scala.collection.mutable.Queue(
          batch1,
          batch2,
          batch3,
          batch4
        )
      )

    // ============================================================
    // PARSE STREAM
    // ============================================================

    val parsedStream =
      inputStream.map { line =>

        val parts =
          line.split(",")

        val airline =
          parts(0)

        val origin =
          parts(1)

        val destination =
          parts(2)

        val delay =
          parts(3).toDouble

        (
          airline,
          origin,
          destination,
          delay
        )
      }

    // ============================================================
    // STATELESS PROCESSING
    // ============================================================

    /*
     * Stateless transformation:
     * Every micro-batch is processed independently.
     */

    val delayedFlights: DStream[(String, String, String, Double)] =
      parsedStream.filter {
        case (
              _,
              _,
              _,
              delay
            ) =>
          delay > 30
      }

    delayedFlights.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println(
          "STATELESS PROCESSING - Delayed Flights"
        )

        rdd.collect().foreach {
          case (
                airline,
                origin,
                destination,
                delay
              ) =>

            println(
              f"$airline%-4s $origin%-4s -> $destination%-4s Delay: $delay%.1f minutes"
            )
        }
      }
    }

    // ============================================================
    // STATEFUL PROCESSING
    // ============================================================

    /*
     * updateStateByKey maintains the cumulative delay count
     * for every airline across multiple micro-batches.
     */

    val carrierEvents =
      parsedStream.map {
        case (
              airline,
              _,
              _,
              _
            ) =>
          (airline, 1)
      }

    val updateFunction =
      (
          newValues: Seq[Int],
          previousState: Option[Int]
      ) => {

        val newCount =
          newValues.sum

        val oldCount =
          previousState.getOrElse(0)

        Some(
          oldCount + newCount
        )
      }

    val cumulativeCarrierCounts =
      carrierEvents.updateStateByKey(
        updateFunction
      )

    cumulativeCarrierCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println(
          "STATEFUL PROCESSING - Cumulative Flights"
        )

        rdd
          .collect()
          .sortBy(_._1)
          .foreach {
            case (
                  airline,
                  count
                ) =>

              println(
                s"$airline -> $count cumulative flights"
              )
          }
      }
    }

    // ============================================================
    // SLIDING WINDOW
    // ============================================================

    /*
     * Window duration = 6 seconds
     * Slide duration  = 2 seconds
     *
     * Therefore, every 2 seconds Spark calculates statistics
     * over the latest 6 seconds of streaming data.
     */

    val windowedStream =
      parsedStream.window(
        Seconds(6),
        Seconds(2)
      )

    val windowedDelayedCounts =
      windowedStream
        .filter {
          case (
                _,
                _,
                _,
                delay
              ) =>
            delay > 30
        }
        .map {
          case (
                airline,
                _,
                _,
                _
              ) =>
            (airline, 1)
        }
        .reduceByKey(
          _ + _
        )

    windowedDelayedCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println(
          "SLIDING WINDOW - Delayed Flights"
        )

        rdd
          .collect()
          .sortBy(_._1)
          .foreach {
            case (
                  airline,
                  count
                ) =>

              println(
                s"$airline -> $count delayed flights in current window"
              )
          }
      }
    }

    // ============================================================
    // STREAMING START
    // ============================================================

    println()
    println("=" * 70)
    println("STARTING STREAMING CONTEXT")
    println("=" * 70)

    streamingContext.start()

    /*
     * Wait long enough for all four micro-batches.
     */

    streamingContext.awaitTerminationOrTimeout(
      12000
    )

    // ============================================================
    // STREAMING STOP
    // ============================================================

    streamingContext.stop(
      stopSparkContext = true,
      stopGracefully = true
    )

    println()
    println("=" * 70)
    println("PART E COMPLETED SUCCESSFULLY")
    println("=" * 70)

    println()
    println("Streaming concepts demonstrated:")
    println("1. DStreams")
    println("2. Micro-batch processing")
    println("3. Batch interval")
    println("4. Stateless transformations")
    println("5. Stateful updateStateByKey")
    println("6. Sliding windows")
    println("7. Window duration")
    println("8. Slide duration")

    println()
    println("=" * 70)
    println("FLIGHTPULSE STREAMING DEMO FINISHED")
    println("=" * 70)
  }
}
