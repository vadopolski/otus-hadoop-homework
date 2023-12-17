package streaming

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.concurrent.duration.DurationInt

object DataSets extends App {
  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master("local")
    .getOrCreate()

  def readFromKafka(): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "input")
      .load()

  case class ClickRecord(timestamp: String, page: String, userId: Long, duration: Int)
  case class ClickBulk(pageType: String, count: Int, totalDuration: Int)
  case class ClickAverage(page: String, averageDuration: Double)

  import spark.implicits._
  implicit val codec: JsonValueCodec[ClickRecord] = JsonCodecMaker.make[ClickRecord]

  def transformToCaseClass(in: DataFrame): Dataset[ClickRecord] =
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]
      .map(readFromString[ClickRecord](_))

  val kafkaSourceDF: DataFrame            = readFromKafka()
  val clickRecordDS: Dataset[ClickRecord] = transformToCaseClass(kafkaSourceDF)

  println(clickRecordDS.isStreaming)

  clickRecordDS.writeStream
    .format("parquet")
    .trigger(Trigger.ProcessingTime(2.seconds))
    .option("path", "src/main/resources/parquet")
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .outputMode(OutputMode.Append())
    .start
    .awaitTermination()

}
