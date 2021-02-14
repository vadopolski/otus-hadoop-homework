package lesson3

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import scala.concurrent.duration._

object LongRidesAlarming {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    val generator = new TaxiRideGenerator()
    val rides = env.addSource(generator)

    val longRides: DataStream[TaxiRide] = rides
      .keyBy(_.rideId)
      .process(new MatchFunction())

    longRides.print()

    env.execute("Long Taxi Rides")
  }

  class MatchFunction extends KeyedProcessFunction[Long, TaxiRide, TaxiRide] {
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext
      .getState(new ValueStateDescriptor[TaxiRide]("ride event", classOf[TaxiRide]))

    override def processElement(
       ride: TaxiRide,
       context: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#Context,
       out: Collector[TaxiRide]
   ): Unit = {
      val previousRideEvent = rideState.value()

      if (previousRideEvent == null) {
        if (ride.isStart) {
          rideState.update(ride)
          context
            .timerService()
            .registerEventTimeTimer(ride.startTime.toEpochMilli + 2.hours.toMillis)
        }
      } else {
        if (!ride.isStart) {
          context
            .timerService()
            .deleteEventTimeTimer(previousRideEvent.startTime.toEpochMilli + 2.hours.toMillis)
          rideState.clear()
        }
      }
    }

    override def onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#OnTimerContext,
        out: Collector[TaxiRide]
    ): Unit = {
      out.collect(rideState.value())
      rideState.clear()
    }

  }


}
