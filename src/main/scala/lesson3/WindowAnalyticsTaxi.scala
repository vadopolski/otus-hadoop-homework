package lesson3

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.concurrent.duration._

object WindowAnalyticsTaxi {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(5)

    val generator = new TaxiFareGenerator()
    val fares = env.addSource(generator)

    val hourlyTolls = fares
      .map((f: TaxiFare) => (f.driverId, f.totalFare))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .reduce(
        (f1: (Long, Float), f2: (Long, Float)) => { (f1._1, f1._2 + f2._2) },
        new WrapWithWindowInfo()
      )

    hourlyTolls.print()

    env.execute("Hourly Fares")
  }

  class WrapWithWindowInfo()
      extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
    override def process(
        key: Long,
        context: Context,
        elements: Iterable[(Long, Float)],
        out: Collector[(Long, Long, Float)]
    ): Unit = {
      val sumOfFares = elements.iterator.next()._2
      out.collect((context.window.getEnd, key, sumOfFares))
    }

  }

}