import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object Project {

  object Haversine {
    import math._
    val R = 6372.8  //radius in km

    // returns distance in km
    def distance(lon1:Double, lat1:Double, lon2:Double, lat2:Double)={
      val dLat=(lat2 - lat1).toRadians
      val dLon=(lon2 - lon1).toRadians
      val a = pow(sin(dLat/2),2) + pow(sin(dLon/2),2) * cos(lat1.toRadians) * cos(lat2.toRadians)
      R * 2 * asin(sqrt(a))
    }
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext("local[*]", "Project")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //Read file and create RDD
    val textFile = sc.textFile("C:/Users/lucka/Desktop/Taxi/train.csv")
    val first = textFile.first()
    val data = textFile
      .filter(row => row != first)
      .filter(x => !x.contains(",,"))
      .filter(x => !x.contains("0,0,0,0"))
      .filter(x => !x.contains(",0"))
      .map(x => x.split(","))

    //Create dataframe and filter invalid values
    val filtered = data
      .toDF()
      .select(concat_ws(",", $"value"))
      .withColumn("_tmp", split($"concat_ws(,, value)", "\\,")).select(
      $"_tmp".getItem(0).as("key"),
      $"_tmp".getItem(1).as("fare_amount"),
      $"_tmp".getItem(2).as("pickup_datetime"),
      $"_tmp".getItem(3).as("pickup_longitude"),
      $"_tmp".getItem(4).as("pickup_latitude"),
      $"_tmp".getItem(5).as("dropoff_longitude"),
      $"_tmp".getItem(6).as("dropoff_latitude"),
      $"_tmp".getItem(7).as("passenger_count")
    ).drop("_tmp")
      .filter(!($"pickup_longitude" === $"pickup_latitude" && $"pickup_latitude" === $"dropoff_longitude" && $"dropoff_longitude" === $"dropoff_latitude"))
      .filter(!($"pickup_longitude" === $"dropoff_longitude" && $"pickup_latitude" === $"dropoff_latitude"))
      .filter(($"pickup_longitude".startsWith("-73.")|| $"pickup_longitude".startsWith("-74.")) && $"pickup_latitude".startsWith("40.") && ($"dropoff_longitude".startsWith("-73.") || $"dropoff_longitude".startsWith("-74.")) && $"dropoff_latitude".startsWith("40."))
      .filter(!(col("fare_amount") < 0.0))
      .filter(Row => Haversine.distance(Row.getString(3).toDouble, Row.getString(4).toDouble, Row.getString(5).toDouble, Row.getString(6).toDouble) >= 1.0)

    val averageForLine = filtered
      .map(Row => Row.getString(1).toDouble / Haversine.distance(Row.getString(3).toDouble, Row.getString(4).toDouble, Row.getString(5).toDouble, Row.getString(6).toDouble) / Row.getString(7).toDouble)

    val dataYears = filtered.withColumn("year", col("key").substr(0,4))
    val df1 = dataYears.withColumn("id",monotonically_increasing_id())
    val df2 = averageForLine.withColumn("id",monotonically_increasing_id())

    val joinDF = df1.join(df2, "id")
    val average = joinDF.groupBy("year").agg(count("year") as "year count", sum("value") as "average sum per year", sum("value")/count("year") as "average per 1 pass / 1 km").sort(asc("year"))
    average.show()

    //most frequent time
    val time = filtered
      .map(Row => Row.getString(0).substring(8, 10) + ", " + Row.getString(0).substring(5, 7) + ", " + Row.getString(0).substring(11, 16))
    val timeDF = time
      .withColumn("_tmp", split($"value", "\\,"))
      .withColumn("day", $"_tmp".getItem(0))
      .withColumn("month", $"_tmp".getItem(1))
      .withColumn("time", $"_tmp".getItem(2))
      .drop("_tmp")
      .drop("value")
    timeDF.groupBy("time").agg(count("time") as "count").sort(desc("count")).show(10)

    //most frequent hours
    val timeHours = filtered
      .map(Row => Row.getString(0).substring(8, 10) + ", " + Row.getString(0).substring(5, 7) + ", " + Row.getString(0).substring(11, 13))
    val timeHoursDF = timeHours
      .toDF()
      .withColumn("_tmp", split($"value", "\\,"))
      .withColumn("day", $"_tmp".getItem(0))
      .withColumn("month", $"_tmp".getItem(1))
      .withColumn("time in hours", $"_tmp".getItem(2))
      .drop("_tmp")
      .drop("value")
    timeHoursDF.groupBy("time in hours").agg(count("time in hours") as "count").sort(desc("count")).show(10)

    //most frequent days
    val days = filtered
      .map(Row => Row.getString(0).substring(8, 10) + ", " + Row.getString(0).substring(5, 7) + ", " + Row.getString(0).substring(11, 13))
    val daysDF = days
      .toDF()
      .withColumn("_tmp", split($"value", "\\,"))
      .withColumn("day", $"_tmp".getItem(0))
      .withColumn("month", $"_tmp".getItem(1))
      .withColumn("time in hours", $"_tmp".getItem(2))
      .drop("_tmp")
      .drop("value")
    daysDF.groupBy("day").agg(count("day") as "count").sort(desc("count")).show(10)

    //most frequent pickup coordinates
    val pickup = filtered
      .map(Row => Row.getString(3) + ", " + Row.getString(4))
    val pickupDF = pickup.toDF("pickup coordinates")
    pickupDF.groupBy("pickup coordinates").agg(count("pickup coordinates") as "count").sort(desc("count")).show(10, false)

    //most frequent dropoff coordinates
    val dropoff = filtered
      .map(Row => Row.getString(5) + ", " + Row.getString(6))
    val dropoffDF = dropoff.toDF("dropoff coordinates")
    dropoffDF.groupBy("dropoff coordinates").agg(count("dropoff coordinates") as "count").sort(desc("count")).show(10, false)


    sc.stop
  }
}