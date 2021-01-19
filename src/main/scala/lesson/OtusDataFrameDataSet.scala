package lesson

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}

object OtusDataFrameDataSet extends App {

  val sparkSession = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  case class TaxiZone(LocationID:   String,
                      Borough:      String,
                      Zone:         String,
                      service_zone:  String)

  val taxiZoneDF = sparkSession.read
    .option("header", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  taxiZoneDF
  .filter(upper(col("service_zone")) === col("service_zone"))
  .groupBy(col("Borough"))
  .count()
  .show()

  import sparkSession.implicits._

  val taxiZoneDS = taxiZoneDF.as[TaxiZone]

//  taxiZoneDS
//    .filter(t => t.service_zone.toUpperCase == t.service_zone)
//    .filter(upper(col("service_zone")) === col("service_zone"))
//    .groupBy(col("Borough"))
//    .count()
//    .show()

}
