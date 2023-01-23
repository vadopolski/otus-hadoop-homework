package streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration.DurationInt

object DataFrame extends App {
  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master("local")
    .getOrCreate()

  spark.readStream.format("rest")




  val schema = StructType(
    Array(
      StructField("timestamp", StringType),
      StructField("page", StringType)
    )
  )

  def readFromKafka(): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "input")
      .load()

  def transform(in: DataFrame): DataFrame =
    in
      .select(expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), schema).as("page")) // composite column (struct)
      .selectExpr("page.timestamp as timestamp", "page.page as page")
      .select(
        date_format(to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"), "HH:mm:ss:SSS")
          .as("time"),
        col("page")
      )

  val kafkaDF = readFromKafka()

  kafkaDF.printSchema()

  val convertedDF = transform(kafkaDF)

  println(convertedDF.isStreaming)

  convertedDF.writeStream
    .foreachBatch { (batch: DataFrame, _: Long) =>
      batch.show(false)
      println(batch.isStreaming) // false
    }
    .trigger(Trigger.ProcessingTime(2.seconds))
//    .trigger(Trigger.Once())
    .start()
    .awaitTermination()
}