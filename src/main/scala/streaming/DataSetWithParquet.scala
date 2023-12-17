package streaming

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.concurrent.duration.DurationInt

object DataSetWithParquet extends App {
  val parallelism = 4

  val spark = SparkSession
    .builder()
    .appName("Integrating Kafka")
    .master(s"local[$parallelism]")
    .getOrCreate()

  val schema = StructType(
    Array(StructField("timestamp", StringType), StructField("page", StringType))
  )

  def readFromKafka(): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "input")
      .load()

  import spark.implicits._

  implicit val codec: JsonValueCodec[ClickRecord] = JsonCodecMaker.make[ClickRecord]

  case class ClickRecord(timestamp: String, page: String, userId: Long, duration: Int)

  def transformToCaseClass(in: DataFrame): Dataset[ClickRecord] =
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]
      .map(readFromString[ClickRecord](_))

  val rawInputDF = readFromKafka()

  val transformedDS: Dataset[ClickRecord] = transformToCaseClass(rawInputDF)

  transformedDS.writeStream
    .format("parquet")
    .trigger(Trigger.ProcessingTime(2.seconds))
    .option("path", "src/main/resources/parquet")
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .outputMode(OutputMode.Append())
    .start
    .awaitTermination()


}
