import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  def createSparkSession(appName: String): SparkSession
}

object SparkUtils extends SparkSessionProvider {

  override def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
  }

  def printSection(title: String): Unit = {
    println()
    println("=" * 70)
    println(s" $title")
    println("=" * 70)
  }

  def classifyDelay(delay: Double): String = {
    delay match {
      case d if d <= 0  => "ON_TIME"
      case d if d <= 15 => "MINOR"
      case d if d <= 60 => "MODERATE"
      case _            => "SEVERE"
    }
  }
}
