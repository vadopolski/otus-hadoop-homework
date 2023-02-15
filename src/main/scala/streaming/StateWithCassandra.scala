package streaming

import com.datastax.oss.driver.api.core.cql
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.spark.connector.cql.CassandraConnector
import com.github.plokhotnyuk.jsoniter_scala.core.{JsonValueCodec, readFromString}
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, GroupState, GroupStateTimeout, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.collection.JavaConversions._
import java.util
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object StateWithCassandra extends App {

  val parallelism = 4

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

  val transformedDS = transformToCaseClass(rawInputDF)

  /// 100 fields
  // 10 field:

  transformedDS
    .repartition(col("userId"))
    .mapPartitions { iter =>
      val connector = CassandraConnector(spark.sparkContext.getConf)

      connector.withSessionDo { session =>
        val set: ResultSet = session.execute("select * from public.click;")

        val result = set
          .all()
          .asScala
          .toList
          .map(r =>
            ClickRecord(
              userId = r.getLong(0),
              timestamp = r.getString(1),
              duration = r.getInt(2),
              page = r.getString(3),
            )
          )

        println(s"Size of state = ${result.length}")

        session.execute(s"""
             |insert into public.click("userId", "timestamp", "page", "duration")
             |values (1, 'test', 'test', 33333) USING TTL 2
          """.stripMargin)

      }

      iter
    }
//    .writeStream
//    .format("kafka")
//    .option("kafka.bootstrap.servers", "localhost:29092")
//    .option("topic", "target-topic")
//    .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
//    .start()
//    .awaitTermination()

//    transformedDS
    .writeStream
    .foreachBatch { (batch: Dataset[ClickRecord], _: Long) =>
      println(s" ====== Batch size is ${batch.count()} ===========")

      batch.show(20, false)
    }
    .option("checkpointLocation", "checkpoints")
    .start()
    .awaitTermination()

}
