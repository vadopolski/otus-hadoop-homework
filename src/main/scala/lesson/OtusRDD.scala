package lesson

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OtusRDD extends App {
  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val context = spark.sparkContext

  case class TaxiZone(
                       LocationID: String,
                       Borough: String,
                       Zone: String,
                       service_zone: String
                     )

  val taxiZoneRDD = context.textFile("src/main/resources/data/taxi_zones.csv")
    .map(l => l.split(","))
    .filter(t => t(3).toUpperCase() == t(3))
    .map(t => TaxiZone(t(0), t(1), t(2), t(3)))
    .map(tz => (tz.Borough, 1))
    .reduceByKey(_ + _)
    .foreach(x => println(s"${x._1} -> ${x._2}"))




}

