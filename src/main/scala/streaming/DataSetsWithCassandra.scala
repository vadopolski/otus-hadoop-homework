package streaming

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import scala.concurrent.duration.DurationInt

object DataSetsWithCassandra extends App {
  val parallelism = 1

  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master(s"local[$parallelism]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.output.consistency.level", "LOCAL_ONE")
    .getOrCreate()

  val schema = StructType(
    Array(StructField("timestamp", StringType), StructField("page", StringType))
  )

  def readFromKafka(): DataFrame =
//    spark.read
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "input")
      .load()

  def transform(in: DataFrame): DataFrame =
    in
      .select(expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), schema).as("page"))
      .selectExpr("page.timestamp as timestamp", "page.page as page")
      .select(
        date_format(to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"), "HH:mm:ss:SSS")
          .as("time"),
        col("page")
      )


  import spark.implicits._
  implicit val codec: JsonValueCodec[ClickRecord] = JsonCodecMaker.make[ClickRecord]

  case class ClickRecord(timestamp: String, page: String, userId: Long, duration: Int)

  def transformToCaseClass(in: DataFrame): Dataset[ClickRecord] =
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]
      .map(readFromString[ClickRecord](_))

  val rawInputDF: DataFrame = readFromKafka()

  val transformedDS: Dataset[ClickRecord] = transformToCaseClass(rawInputDF)


  transformedDS.writeStream
      .foreachBatch { (batch: Dataset[ClickRecord], id: Long) =>
            println(s"Batch is stream = ${batch.isStreaming} id = $id count = ${batch.count()}")

            batch
              .select($"userId", $"page", $"duration", $"timestamp")
//              .show(30, false)

              .write
              .cassandraFormat("click", "public")
              .mode(SaveMode.Append)
              .save()
    }
    .trigger(Trigger.Once())
//    .trigger(Trigger.ProcessingTime(1.seconds))
    .start()
    .awaitTermination()

}
