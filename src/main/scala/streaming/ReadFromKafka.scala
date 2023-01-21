package streaming

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.concurrent.duration.DurationInt

object ReadFromKafka extends App {
  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

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

  import spark.implicits._

  def transformToCaseClass(in: DataFrame): Dataset[String] =
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]

  def writeDsToConsole(in: Dataset[String]): Unit =
    in.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()

  val kafkaDF = readFromKafka()

  val transformedDS = transformToCaseClass(kafkaDF)

  writeDsToConsole(transformedDS)
}