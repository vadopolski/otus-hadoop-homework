package lesson3

import lesson3.ExerciseBase.rideSourceOrTest
import org.apache.flink.streaming.api.scala._


object CityTaxiFiltering {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(5)


    val rides: DataStream[TaxiRide] = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))


    val filteredRides: DataStream[TaxiRide] = rides
      .filter(ride => GeoUtils.isInNYC(ride.startLon, ride.startLat)
        && GeoUtils.isInNYC(ride.endLon, ride.endLat))


    filteredRides.print()


    env.execute("Taxi Ride Filtering")
  }
}