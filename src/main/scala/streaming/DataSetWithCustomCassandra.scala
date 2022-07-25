package streaming

import com.datastax.spark.connector.cql.CassandraConnector
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.DurationInt


object DataSetWithCustomCassandra extends App {
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
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("startingOffsets", "latest")
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

  val rawInputDF: DataFrame = readFromKafka()

  val transformedDS: Dataset[ClickRecord] = transformToCaseClass(rawInputDF)

  writeToCassandra()

  def writeToCassandraInBatches(): Unit = {
    transformedDS.writeStream
      .foreachBatch { (batch: Dataset[ClickRecord], id: Long) =>
        println(s"Batch is stream = ${batch.isStreaming} id = $id count = ${batch.count()}")

        batch
          .select($"userId", $"page", $"duration", $"timestamp")
          //        .show(30, false)
          .write
          .cassandraFormat("click", "public")
          .mode(SaveMode.Append)
          .save()
      }
      //    .trigger(Trigger.Once())
      .trigger(Trigger.ProcessingTime(1.seconds))
      .start()
      .awaitTermination()
  }

  def writeToCassandra(): Unit = {
    transformedDS
      .writeStream
      .foreach(new ClickCassandraForeachWriter)
      .trigger(Trigger.ProcessingTime(1.seconds))
      .start()
      .awaitTermination()
  }


  class ClickCassandraForeachWriter extends ForeachWriter[ClickRecord] {
    val keyspace = "public"
    val table = "click"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open connection")
      true
    }

    override def process(car: ClickRecord): Unit = {
      println(s"class is $car")

      connector.withSessionDo { session =>
        val insertSQL =
          s"""
             |insert into $keyspace.$table("userId", "timestamp", "page", "duration")
             |values (${car.userId}, '${car.timestamp}', '${car.page}', ${car.duration}) if not exists
           """.stripMargin

        println(insertSQL)
        session.execute(insertSQL)




      }
    }

    override def close(errorOrNull: Throwable): Unit = println("Closing connection")

  }

}

// TODO: Added checking
//        val prep = connector.prepare(session, insertSQL)
//        val res = session.execute(prep.bind(barcode, dateProcessed))
//        if (res.wasApplied) {
//          (barcode, dateProcessed)
//        }
//        else {
//          val data = res.one()
//          (data.getString("bar_code"), data.getString("reg_date"))
//        }
