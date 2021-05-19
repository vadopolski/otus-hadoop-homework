package lesson3

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time



object RidesJoinFares {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(5)

    val fares = env
      .addSource(new TaxiFareGenerator())
      .keyBy( f => f.rideId)

    val rides = env
      .addSource(new TaxiRideGenerator())
      .filter(r => r.isStart)
      .keyBy(r => r.rideId)

    val result = rides
      .join(fares)
      .where(r => r.rideId)
      .equalTo(f => f.rideId)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
      .apply {(r, f) => r + "," + f}

    result.print()

    env.execute()

  }
}