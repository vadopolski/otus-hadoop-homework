package lesson

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OtusRDD extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val context: SparkContext = spark.sparkContext

  case class TaxiZone(locationId: Int,
                      borough: String,
                      zone: String,
                      serviceZone: String)

  val taxiZoneRDD = context.textFile("src/main/resources/data/taxi_zones.csv")
    .map(l => l.split(","))
    .filter(t => t(3).toUpperCase() == t(3))
    .map(t => TaxiZone(t(0).toInt, t(1), t(2), t(3)))
    .map(tz => (tz.borough, 1))
    .reduceByKey(_ + _)
    .foreach(x => println(s"${x._1} -> ${x._2}"))




}

