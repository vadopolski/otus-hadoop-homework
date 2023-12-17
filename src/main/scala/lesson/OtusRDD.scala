package lesson

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OtusRDD extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local[2]")
    .getOrCreate()

  val context: SparkContext = spark.sparkContext

/*

1 17 23.02.22
2 18 24.02.22
3 24 25.02.22

2 record
lead lag



  core1  t1 t2 t3
  core2  t4 t5 t6
  core3  t7 t8 ...
  core4
  core5
  core6
  core4
  core5
  core6

*/


  case class TaxiZone(
                       LocationID: String,
                       Borough: String,
                       Zone: String,
                       service_zone: String
                     )

  val value = context.textFile("src/main/resources/data/taxi_zones.csv")
    .map(l => l.split(","))
    .filter(t => t(3).toUpperCase() == t(3))
    .map(t => TaxiZone(t(0), t(1), t(2), t(3)))
    .map(tz => (tz.Borough, 1))
    .reduceByKey(_ + _)

  val taxiZoneRDD = value
    .foreach(x => println(s"${x._1} -> ${x._2}"))




}

