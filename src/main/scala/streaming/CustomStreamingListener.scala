package streaming

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture}
import scala.collection.JavaConverters.mapAsScalaMapConverter

class CustomStreamingListener extends StreamingQueryListener {

  private val executorService: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor
  private var scheduledFuture: ScheduledFuture[_] = _

  // register the source with the Spark metrics system
//  spark.sparkContext.env.metricsSystem.registerSource(myMetricSource)

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println("Query started: onQueryStarted")
    //    scheduledFuture = executorService.scheduleWithFixedDelay(() => {
    //      ALERTER.alertWarn("There are some processes that are either stuck or take more time than usual")
    //    }, 30, 15, TimeUnit.MINUTES)
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println("Query started: onQueryProgress")
    println(s"numInputRows = ${event.progress.numInputRows}")
    println(s"inputRowsPerSecond = ${event.progress.inputRowsPerSecond.toString}")
    println(s"processedRowsPerSecond = ${event.progress.processedRowsPerSecond.toString}")
    println(s"durationMs = ${event.progress.durationMs.toString}")
    println(s"stateOperators = ${event.progress.stateOperators.toString}")

    val valuesMap = event.progress.observedMetrics.asScala.toMap
    scheduledFuture.cancel(true)

//    sendStreamingMetrics(event)
    //    commitConsumerOffsets(event)
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    println("Query started: onQueryTerminated")
    //    ALERTER.alertError("Unfortunately, streaming query is stopped, with or without error")
  }

  private def sendStreamingMetrics(event: QueryProgressEvent): Unit = {
    println(s"Query started: sendStreamingMetrics + $event")
//    REPORTER.forRegistry(s"pa.analytics.jobs.${configs.getApplicationName}")
//      .register("numInputRows", event.progress.numInputRows.toString)
//      .register("inputRowsPerSecond", event.progress.inputRowsPerSecond.toString)
//      .send()
  }
}